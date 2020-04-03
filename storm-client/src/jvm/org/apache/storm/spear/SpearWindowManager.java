package org.apache.storm.spear;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.storm.spear.sample.CongressUtils;
import org.apache.storm.spear.window.ReservoirSample;
import org.apache.storm.spear.window.TimeWindow;
import org.apache.storm.spear.window.WindowAssignUtils;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class SpearWindowManager<K> implements TriggerHandler, ISpearWindowManager {
  
  private static final Logger LOG = LoggerFactory.getLogger(SpearWindowManager.class);
  
  private static final String EVICTION_STATE_KEY = "es";
  
  private static final String TRIGGER_STATE_KEY = "ts";
  
  public static final int EXPIRE_EVENTS_THRESHOLD = 100;
  
  protected final Collection<Event<Tuple>> queue;
  
  private final ReentrantLock lock;
  
  private final List<Tuple> expiredEvents;
  private final Set<Tuple> prevWindowEvents;
  private final AtomicInteger eventsSinceLastExpiry;
  
  protected EvictionPolicy<Tuple, ?> evictionPolicy;
  protected TriggerPolicy<Tuple, ?> triggerPolicy;
  
  protected final SpearWindowLifecycleListener<K> lifecycleListener;
  
  private final int budget;
  private final float error;
  private final float confidence;
  
  private Function<Object, K> keyExtractor;
  private Function<Object, Number> valueExtractor;
  
  private long windowRange;
  private long windowSlide;
  private BaseWindowedBolt.Duration windowLengthDuration;
  
  private final double zp;
  
  private Map<TimeWindow, WindowStatistics<K>> timeWindows;
  
  private final K[] keys;
  
  private final int budgetPerKey;
  
  private SpearBolt<K> bolt;
  
  public SpearWindowManager(SpearWindowLifecycleListener<K> lifecycleListener,
                            Collection<Event<Tuple>> queue,
                            Function<Object, K> keyExtractor,
                            Function<Object, Number> valueExtractor,
                            SpearBolt<K> bolt, int budget, float error,
                            float confidence, K[] keys) {
    if (budget < .0f) {
      throw new IllegalArgumentException("budget must be a long greater than 0.");
    }
    if (error < .0f || error >= 1.0f) {
      throw new IllegalArgumentException("error must be in [0, 1).");
    }
    if (confidence < .0f || confidence > 1.0f) {
      throw new IllegalArgumentException("confidence must be in [0, 1].");
    }
    this.expiredEvents = new ArrayList<>();
    this.prevWindowEvents = new HashSet<>();
    this.lifecycleListener = lifecycleListener;
    this.queue = queue;
    this.eventsSinceLastExpiry = new AtomicInteger();
    this.lock = new ReentrantLock(true);
    this.bolt = bolt;
    this.budget = budget;
    this.error = error;
    this.confidence = confidence;
    this.keyExtractor = keyExtractor;
    this.valueExtractor = valueExtractor;
    NormalDistribution standardNormal = new NormalDistribution(0.0f, 1.0f);
    this.zp = standardNormal.inverseCumulativeProbability(confidence);
    this.timeWindows = new HashMap<>();
    // Constrained fields
    if (keys != null && keys.length > 0) {
      this.keys = keys.clone();
      this.budgetPerKey = this.budget / keys.length;
    } else {
      this.keys = null;
      this.budgetPerKey = -1;
    }
  }
  
  public void setEvictionPolicy(EvictionPolicy<Tuple, ?> evictionPolicy) {
    this.evictionPolicy = evictionPolicy;
  }
  
  public void setTriggerPolicy(TriggerPolicy<Tuple, ?> triggerPolicy) {
    this.triggerPolicy = triggerPolicy;
  }
  
  public void setSlidingWindowParameters(long size, long slide) {
    this.windowRange = size;
    this.windowSlide = slide;
  }
  
  public void setWindowLengthDuration(BaseWindowedBolt.Duration duration) {
    this.windowLengthDuration = duration;
  }
  
  public void add(Tuple event) {
    add(event, System.currentTimeMillis());
  }
  
  public void add(Tuple event, long timestamp) {
    add(new EventImpl<>(event, timestamp));
  }
  
  public void add(Event<Tuple> tuple) {
    if (!tuple.isWatermark()) {
      Tuple t = tuple.get();
      try {
        lock.lock();
        queue.add(tuple);
        K key = keyExtractor.apply(t);
        Number value = valueExtractor.apply(t);
        long timestamp = tuple.getTimestamp();
        Collection<TimeWindow> assignedWindows = WindowAssignUtils.assignWindows(windowRange,
            windowSlide, timestamp);
        for (TimeWindow w : assignedWindows) {
          if (this.budgetPerKey < 0) {
            if (timeWindows.containsKey(w)) {
              timeWindows.get(w).add(key);
            } else {
              WindowStatistics<K> f = new WindowStatistics<>(budget);
              f.add(key);
              timeWindows.put(w, f);
            }
          } else {
            if (timeWindows.containsKey(w)) {
              timeWindows.get(w).add(key, value);
            } else {
              WindowStatistics<K> sample = new WindowStatistics<>(budget, budgetPerKey);
              sample.add(key, value);
              timeWindows.put(w, sample);
            }
          }
        }
      } finally {
        lock.unlock();
      }
    } else {
      LOG.debug("received watermark with timestamp: {}", tuple.getTimestamp());
    }
    track(tuple);
    compactWindow();
  }
  
  @Override
  public boolean onTrigger() {
    if (budgetPerKey < 0) {
      return onTriggerDefault();
    }
    return false;
  }
  
  private boolean onTriggerDefault() {
    List<Tuple> windowTuples;
    List<Tuple> expired = null;
    boolean accelerate = false;
    double error = 1.0f;
    HashMap<K, ReservoirSample> congressSample = null;
    Map<K, Number> result = null;
    WindowStatistics<K> sample;
    long windowEnd = evictionPolicy.getContext().getReferenceTime();
    long windowStart = getWindowStartTs(windowEnd);
    TimeWindow window = new TimeWindow(windowStart, windowEnd);
    try {
      lock.lock();
      /*
       * scan the entire window to handle out of order events in
       * the case of time based windows.
       */
      sample = timeWindows.remove(window);  //  O(1): remove record from map.
      windowTuples = scanEvents(true, null);
      expired = new ArrayList<>(expiredEvents);
      expiredEvents.clear();
    } finally {
      lock.unlock();
    }
    // SPEAr overhead: O(number of distinct groups * sample size)
    if (sample != null && sample.getNumberOfTuples() > 0 && !sample.overflow()) {
      congressSample = CongressUtils.sample(budget, windowTuples, sample.getHistogram(),
          keyExtractor, valueExtractor);
      result = new HashMap<>(congressSample.size());
      error = bolt.estimateError(congressSample, result).doubleValue();
      /**
       * The following check is to ensure that there is at least one tuple in the
       * budget for each group.
       */
      accelerate = (error <= this.error && budget >= sample.getNumberOfKeys());
    }
    if (windowTuples.size() > 0) {
      if (accelerate) {
        lifecycleListener.onAcceleratedActivation(windowTuples, windowStart, windowEnd, result);
      } else {
        List<Tuple> newEvents = new ArrayList<>();
        for (Tuple t : windowTuples) {
          if (!prevWindowEvents.contains(t)) {
            newEvents.add(t);
          }
        }
        prevWindowEvents.clear();
        prevWindowEvents.addAll(windowTuples);
        lifecycleListener.onActivation(windowTuples, newEvents, expired, windowEnd);
      }
    }
    triggerPolicy.reset();
    return !windowTuples.isEmpty();
  }
  
  protected void track(Event<Tuple> event) {
    evictionPolicy.track(event);
    triggerPolicy.track(event);
  }
  
  private void compactWindow() {
    if (eventsSinceLastExpiry.incrementAndGet() >= EXPIRE_EVENTS_THRESHOLD) {
      scanEvents(false, null);
    }
  }
  
  private List<Tuple> scanEvents(boolean fullScan, final HashMap<K, Integer> frequencyMap) {
    LOG.debug("Scan tuples, eviction policy {}", evictionPolicy);
    List<Tuple> tuplesToExpire = new ArrayList<>();
    List<Tuple> tuplesToProcess = new ArrayList<>();
    try {
      lock.lock();
      Iterator<Event<Tuple>> it = queue.iterator();
      while (it.hasNext()) {
        Event<Tuple> tuple = it.next();
        EvictionPolicy.Action action = evictionPolicy.evict(tuple);
        if (action == EvictionPolicy.Action.EXPIRE) {
          Tuple t = tuple.get();
          tuplesToExpire.add(t);
          it.remove();
        } else if (!fullScan || action == EvictionPolicy.Action.STOP) {
          break;
        } else if (action == EvictionPolicy.Action.PROCESS) {
          tuplesToProcess.add(tuple.get());
          if (frequencyMap != null) {
            K key = keyExtractor.apply(tuple.get());
            frequencyMap.compute(key, (k, v) -> v == null ? 1 : v + 1);
          }
        }
      }
      expiredEvents.addAll(tuplesToExpire);
    } finally {
      lock.unlock();
    }
    eventsSinceLastExpiry.set(0);
    LOG.debug("{} tuples expired from window.", tuplesToExpire.size());
    if (!tuplesToExpire.isEmpty()) {
      LOG.debug("invoking " + lifecycleListener.getClass().getSimpleName() + ".onExpiry()");
      lifecycleListener.onExpiry(tuplesToExpire);
    }
    return tuplesToProcess;
  }
  
  private Long getWindowStartTs(Long endTs) {
    Long res = null;
    if (endTs != null && windowLengthDuration != null)
      res = endTs - windowLengthDuration.value;
    return res;
  }
  
  public void shutdown() {
    if (triggerPolicy != null) {
      triggerPolicy.shutdown();
    }
  }
  
  /**
   * Scan the event queue and return the next earliest event timestamp between the given timestamps.
   * @param startTs the start timestamp (exclusive)
   * @param endTs the end timestamp (inclusive)
   * @return the earliest event with timestamp between startTs and endTs
   */
  public long getEarliestEventTs(long startTs, long endTs) {
    long minTs = Long.MAX_VALUE;
    for (Event<Tuple> event : queue)
      if (event.getTimestamp() > startTs && event.getTimestamp() <= endTs)
        minTs = Math.min(minTs, event.getTimestamp());
    return minTs;
  }
  
  /**
   * Scan the event queue and return the number of events having timestamp less than or equal to
   * a reference time.
   * @param referenceTime the reference timestamp in milliseconds
   * @return the count of events with timestamp less than or equal to reference time
   */
  public int getEventCount(long referenceTime) {
    int count = 0;
    for (Event<Tuple> event : queue)
      if (event.getTimestamp() <= referenceTime)
        ++count;
    return count;
  }
  
  public List<Long> getSlidingWindowCountTimestamps(long startTs, long endTs, int slidingCount) {
    List<Long> timestamps = new ArrayList<>();
    if (endTs > startTs) {
      int count = 0;
      long ts = Long.MIN_VALUE;
      for (Event<Tuple> event : queue) {
        if (event.getTimestamp() > startTs && event.getTimestamp() <= endTs) {
          ts = Math.max(ts, event.getTimestamp());
          if (++count % slidingCount == 0)
            timestamps.add(ts);
        }
      }
    }
    return timestamps;
  }
  
  public void restoreState(Map<String, Optional<?>> state) {
    Optional.ofNullable(state.get(EVICTION_STATE_KEY))
        .flatMap(x -> x)
        .ifPresent(v -> ((EvictionPolicy) evictionPolicy).restoreState(v));
    Optional.ofNullable(state.get(TRIGGER_STATE_KEY))
        .flatMap(x -> x)
        .ifPresent(v -> ((TriggerPolicy) triggerPolicy).restoreState(v));
  }
  
  public Map<String, Optional<?>> getState() {
    return ImmutableMap.of(
        EVICTION_STATE_KEY, Optional.ofNullable(evictionPolicy.getState()),
        TRIGGER_STATE_KEY, Optional.ofNullable(triggerPolicy.getState())
    );
  }
}
