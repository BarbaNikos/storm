package org.apache.storm.spear;

import com.google.common.collect.ImmutableMap;
import org.apache.storm.spear.window.ReservoirSample;
import org.apache.storm.spear.window.TimeWindow;
import org.apache.storm.spear.window.WindowAssignUtils;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class SpearScalarWindowManager implements TriggerHandler, ISpearWindowManager {
  
  private static final Logger LOG = LoggerFactory.getLogger(SpearScalarWindowManager.class);
  private static final String EVICTION_STATE_KEY = "es";
  private static final String TRIGGER_STATE_KEY = "ts";
  public static final int EXPIRE_EVENTS_THRESHOLD = 100;
  protected final Collection<Event<Tuple>> queue;
  private final ReentrantLock lock;
  private final List<Tuple> expiredEvents;
  private final Set<Event<Tuple>> prevWindowEvents;
  private final AtomicInteger eventsSinceLastExpiry;
  protected EvictionPolicy<Tuple, ?> evictionPolicy;
  protected TriggerPolicy<Tuple, ?> triggerPolicy;
  protected final ScalarSpearWindowLifecycleListener lifecycleListener;
  
  private final int budget;
  private final float error;
  private final float confidence;
  
  private ScalarAggregate aggregate;
  private Function<Object, Number> extractor;
  private long windowRange;
  private long windowSlide;
  private BaseWindowedBolt.Duration windowLengthDuration;
  private Map<TimeWindow, ReservoirSample> samples;
  
  public SpearScalarWindowManager(ScalarSpearWindowLifecycleListener lifecycleListener,
                                  ScalarAggregate aggregate,
                                  Function<Object, Number> fieldExtractor,
                                  int budget, float error, float confidence) {
    this(lifecycleListener, new ConcurrentLinkedDeque<>(), aggregate, fieldExtractor, budget,
        error, confidence);
  }
  
  public SpearScalarWindowManager(ScalarSpearWindowLifecycleListener lifecycleListener,
                                  Collection<Event<Tuple>> queue,
                                  ScalarAggregate aggregate,
                                  Function<Object, Number> fieldExtractor,
                                  int budget, float error, float confidence) {
    if (budget < .0f)
      throw new IllegalArgumentException("budget must be a long greater than 0.");
    if (error < .0f || error >= 1.0f)
      throw new IllegalArgumentException("error must be in [0, 1).");
    if (confidence < .0f || confidence > 1.0f)
      throw new IllegalArgumentException("confidence must be in [0, 1].");
    this.lifecycleListener = lifecycleListener;
    this.queue = queue;
    lock = new ReentrantLock(true);
    this.budget = budget;
    this.error = error;
    this.confidence = confidence;
    this.aggregate = aggregate;
    this.extractor = fieldExtractor;
    expiredEvents = new ArrayList<>();
    prevWindowEvents = new HashSet<>();
    eventsSinceLastExpiry = new AtomicInteger();
    samples = new HashMap<>();
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
  
  public void add(Tuple event, long ts) {
    add(new EventImpl<>(event, ts));
  }
  
  // TODO(nkatsip): I DID CHANGES IN THIS ONE
  public void add(Event<Tuple> tuple) {
    if (!tuple.isWatermark()) {
      try {
        lock.lock();
        queue.add(tuple);
        Number value = extractor.apply(tuple.get());
        long timestamp = tuple.getTimestamp();
        List<TimeWindow> assignedWindows = WindowAssignUtils.assignWindows(windowRange, windowSlide,
            timestamp);
        for (TimeWindow w : assignedWindows) {
          if (samples.containsKey(w)) {
            samples.get(w).addValue(value);
          } else {
            ReservoirSample sample = new ReservoirSample(budget);
            sample.addValue(value);
            samples.put(w, sample);
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
    List<Tuple> windowTuples;
    boolean accelerate = false;
    ReservoirSample sample = null;
    ArrayList<Number> result = new ArrayList<>(1);
    long windowEnd = evictionPolicy.getContext().getReferenceTime();
    long windowStart = getWindowStartTs(windowEnd);
    TimeWindow window = new TimeWindow(windowStart, windowEnd);
    try {
      lock.lock();
      windowTuples = scanEvents(true);
      if (samples.containsKey(window)) {
        sample = samples.remove(window);
      }
    } finally {
      lock.unlock();
    }
    if (windowTuples.size() > 0) {
      if (sample != null) {
        accelerate = aggregate.confidenceIntervalTest(sample.getSample(), sample.getSampleIndex(),
            this.error, this.confidence, (int) sample.getCount(), result);
      }
      if (accelerate && result.size() > 0) {
        lifecycleListener.onExpeditedActivation(windowTuples, windowStart, windowEnd,
            result.get(0));
      } else {
        lifecycleListener.onActivation(windowTuples, null, null, windowEnd);
      }
    }
    triggerPolicy.reset();
    return !windowTuples.isEmpty();
  }
  
  private List<Tuple> scanEvents(boolean fullScan) {
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
          tuplesToExpire.add(tuple.get());
          it.remove();
        } else if (!fullScan || action == EvictionPolicy.Action.STOP) {
          break;
        } else if (action == EvictionPolicy.Action.PROCESS) {
          tuplesToProcess.add(tuple.get());
        }
      }
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
  
  /**
   * Scans the event queue and returns the next earliest event ts
   * between the startTs and endTs
   *
   * @param startTs the start ts (exclusive)
   * @param endTs the end ts (inclusive)
   * @return the earliest event ts between startTs and endTs
   */
  public long getEarliestEventTs(long startTs, long endTs) {
    long minTs = Long.MAX_VALUE;
    for (Event<Tuple> event : queue) {
      if (event.getTimestamp() > startTs && event.getTimestamp() <= endTs) {
        minTs = Math.min(minTs, event.getTimestamp());
      }
    }
    return minTs;
  }
  
  /**
   * Scans the event queue and returns number of events having
   * timestamp less than or equal to the reference time.
   *
   * @param referenceTime the reference timestamp in millis
   * @return the count of events with timestamp less than or equal to referenceTime
   */
  public int getEventCount(long referenceTime) {
    int count = 0;
    for (Event<Tuple> event : queue) {
      if (event.getTimestamp() <= referenceTime) {
        ++count;
      }
    }
    return count;
  }
  
  /**
   * Scans the event queue and returns the list of event ts
   * falling between startTs (exclusive) and endTs (inclusive)
   * at each sliding interval counts.
   *
   * @param startTs the start timestamp (exclusive)
   * @param endTs the end timestamp (inclusive)
   * @param slidingCount the sliding interval count
   * @return the list of event ts
   */
  public List<Long> getSlidingWindowCountTimestamps(long startTs, long endTs, int slidingCount) {
    List<Long> timestamps = new ArrayList<>();
    if (endTs > startTs) {
      int count = 0;
      long ts = Long.MIN_VALUE;
      for (Event<Tuple> event : queue) {
        if (event.getTimestamp() > startTs && event.getTimestamp() <= endTs) {
          ts = Math.max(ts, event.getTimestamp());
          if (++count % slidingCount == 0) {
            timestamps.add(ts);
          }
        }
      }
    }
    return timestamps;
  }
  
  public void shutdown() {
    LOG.debug("Shutting down {}", SpearScalarWindowManager.class.getSimpleName());
    if (triggerPolicy != null) {
      triggerPolicy.shutdown();
    }
  }
  
  /**
   * expires events that fall out of the window every
   * EXPIRE_EVENTS_THRESHOLD so that the window does not grow
   * too big.
   */
  private void compactWindow() {
    if (eventsSinceLastExpiry.incrementAndGet() >= EXPIRE_EVENTS_THRESHOLD) {
      scanEvents(false);
    }
  }
  
  /**
   * feed the event to the eviction and trigger policies
   * for bookkeeping and optionally firing the trigger.
   */
  private void track(Event<Tuple> windowEvent) {
    evictionPolicy.track(windowEvent);
    triggerPolicy.track(windowEvent);
  }
  
  private Long getWindowStartTs(Long endTs) {
    Long res = null;
    if (endTs != null && windowLengthDuration != null)
      res = endTs - windowLengthDuration.value;
    return res;
  }
  
  @Override
  public String toString() {
    return String.format("%s{evictionPolicy=%s, triggerPolicy=%s}",
        SpearScalarWindowManager.class.getSimpleName(), evictionPolicy, triggerPolicy);
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
