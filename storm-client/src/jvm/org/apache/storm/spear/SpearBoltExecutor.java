package org.apache.storm.spear;

import org.apache.storm.Config;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Supplier;

public class SpearBoltExecutor<K> implements IRichBolt {
  
  public static final String LATE_TUPLE_FIELD = "late_tuple";
  private static final Logger LOG = LoggerFactory.getLogger(SpearBoltExecutor.class);
  private static final int DEFAULT_WATERMARK_EVENT_INTERVAL_MS = 1000;
  private static final int DEFAULT_MAX_LAG_MS = 0; // no lag
  private final SpearBolt<K> bolt;
  private Function<Object, K> keyExtractor;
  private Function<Object, Number> valueExtractor;
  private int budget;
  private final float error;
  private final float confidence;
  
  transient SpearWatermarkEventGenerator watermarkEventGenerator;
  private transient WindowedOutputCollector windowedOutputCollector;
  private transient SpearWindowLifecycleListener<K> listener;
  private transient SpearWindowManager<K> windowManager;
  private transient int maxLagMs;
  private TimestampExtractor timestampExtractor;
  private transient String lateTupleStream;
  private transient TriggerPolicy<Tuple, ?> triggerPolicy;
  private transient EvictionPolicy<Tuple, ?> evictionPolicy;
  private transient BaseWindowedBolt.Duration windowLengthDuration;
  
  private final K[] groups;
  
  public SpearBoltExecutor(SpearBolt<K> bolt,
                           Function<Object, K> keyExtractor,
                           Function<Object, Number> valueExtractor,
                           int budget,
                           float error,
                           float confidence) {
    this(bolt, keyExtractor, valueExtractor, budget, error, confidence, null);
  }
  
  public SpearBoltExecutor(SpearBolt<K> bolt,
                           Function<Object, K> keyExtractor,
                           Function<Object, Number> valueExtractor,
                           int budget,
                           float error,
                           float confidence,
                           K[] groups) {
    this.bolt = bolt;
    this.timestampExtractor = bolt.getTimestampExtractor();
    this.keyExtractor = keyExtractor;
    this.valueExtractor = valueExtractor;
    this.budget = budget;
    this.error = error;
    this.confidence = confidence;
    this.groups = groups;
  }
  
  private SpearWindowManager<K> initWindowManager(SpearWindowLifecycleListener<K> lifecycleListener,
      Map<String, Object> conf, TopologyContext context, Collection<Event<Tuple>> queue,
      boolean stateful) {
    SpearWindowManager<K> manager = new SpearWindowManager<>(lifecycleListener, queue,
        keyExtractor, valueExtractor, bolt, budget, error, confidence, groups);
    BaseWindowedBolt.Count windowLengthCount = BoltExecutorUtils.getWindowLengthCount(conf);
    BaseWindowedBolt.Count slidingIntervalCount = BoltExecutorUtils.getSlidingIntervalCount(conf);
    windowLengthDuration = BoltExecutorUtils.getWindowLengthDuration(conf);
    BaseWindowedBolt.Duration slidingIntervalDuration = BoltExecutorUtils.getSlidingIntervalDuration(conf);
    if (slidingIntervalCount == null && slidingIntervalDuration == null) {
      slidingIntervalCount = new BaseWindowedBolt.Count(1);
    }
    BoltExecutorUtils.validateLateTupleStream(timestampExtractor, conf, context);
    if (timestampExtractor != null) {
      maxLagMs = BoltExecutorUtils.getMaximumLagInMillis(timestampExtractor, conf);
      int watermarkInterval = BoltExecutorUtils.getWatermarkIntervalInMillis(
          timestampExtractor, conf);
      watermarkEventGenerator = new SpearWatermarkEventGenerator(manager, watermarkInterval,
          maxLagMs, BoltExecutorUtils.getComponentStreams(context));
    }
    BoltExecutorUtils.validate(conf, windowLengthCount, windowLengthDuration, slidingIntervalCount,
        slidingIntervalDuration);
    evictionPolicy = getEvictionPolicy(windowLengthCount, windowLengthDuration);
    triggerPolicy = getTriggerPolicy(slidingIntervalCount, slidingIntervalDuration, manager,
        evictionPolicy);
    manager.setWindowLengthDuration(windowLengthDuration);
    if (windowLengthDuration != null && slidingIntervalDuration != null)
      manager.setSlidingWindowParameters(windowLengthDuration.value, slidingIntervalDuration.value);
    manager.setEvictionPolicy(evictionPolicy);
    manager.setTriggerPolicy(triggerPolicy);
    return manager;
  }
  
  private void restoreState(Map<String, Optional<?>> state) {
    windowManager.restoreState(state);
  }
  
  private Map<String, Optional<?>> getState() {
    return windowManager.getState();
  }
  
  protected void start() {
    if (watermarkEventGenerator != null) {
      LOG.debug("starting watermark event generator");
      watermarkEventGenerator.start();
    }
    LOG.debug("Starting trigger policy");
    triggerPolicy.start();
  }
  
  TriggerPolicy<Tuple, ?> getTriggerPolicy(BaseWindowedBolt.Count slidingIntervalCount,
                                           BaseWindowedBolt.Duration slidingIntervalDuration,
                                           SpearWindowManager<K> manager,
                                           EvictionPolicy<Tuple, ?> evictionPolicy) {
    if (slidingIntervalCount != null) {
      if (isTupleTs()) {
        return new SpearWatermarkCountTriggerPolicy<>(slidingIntervalCount.value, manager,
            evictionPolicy, manager);
      } else {
        return new CountTriggerPolicy<>(slidingIntervalCount.value, manager, evictionPolicy);
      }
    } else {
      if (isTupleTs()) {
        return new SpearWatermarkTimeTriggerPolicy<>(slidingIntervalDuration.value, manager,
            evictionPolicy, manager);
      } else {
        return new TimeTriggerPolicy<>(slidingIntervalDuration.value, manager, evictionPolicy);
      }
    }
  }
  
  EvictionPolicy<Tuple, ?> getEvictionPolicy(BaseWindowedBolt.Count windowLengthCount,
                                             BaseWindowedBolt.Duration windowLengthDuration) {
    if (windowLengthCount != null) {
      if (isTupleTs()) {
        return new WatermarkCountEvictionPolicy<>(windowLengthCount.value);
      } else {
        return new CountEvictionPolicy<>(windowLengthCount.value);
      }
    } else {
      if (isTupleTs()) {
        return new WatermarkTimeEvictionPolicy<>(windowLengthDuration.value, maxLagMs);
      } else {
        return new TimeEvictionPolicy<>(windowLengthDuration.value);
      }
    }
  }
  
  @Override
  public void prepare(Map<String, Object> conf, TopologyContext context,
                      OutputCollector collector) {
    doPrepare(conf, context, collector, new ConcurrentLinkedQueue<>(), false);
  }
  
  protected void doPrepare(Map<String, Object> conf, TopologyContext context,
                           OutputCollector collector, Collection<Event<Tuple>> queue,
                           boolean stateful) {
    Objects.requireNonNull(conf);
    Objects.requireNonNull(context);
    Objects.requireNonNull(collector);
    Objects.requireNonNull(queue);
    this.windowedOutputCollector = new WindowedOutputCollector(collector);
    this.bolt.prepare(conf, context, windowedOutputCollector);
    this.listener = newWindowLifecycleListener();
    this.windowManager = initWindowManager(listener, conf, context, queue, stateful);
    start();
    LOG.info("Initialized approx-window manager {} ", windowManager.getClass().getSimpleName());
  }
  
  private boolean isTupleTs() {
    return timestampExtractor != null;
  }
  
  @Override
  public void execute(Tuple input) {
    if (isTupleTs()) {
      long ts = timestampExtractor.extractTimestamp(input);
      if (watermarkEventGenerator.track(input.getSourceGlobalStreamId(), ts)) {
        windowManager.add(input, ts);
      } else {
        if (lateTupleStream != null) {
          windowedOutputCollector.emit(lateTupleStream, input, new Values(input));
        } else {
          LOG.info("Received a late tuple {} with ts {}. This will not be processed.", input, ts);
        }
        windowedOutputCollector.ack(input);
      }
    } else {
      windowManager.add(input);
    }
  }
  
  @Override
  public void cleanup() {
    if (watermarkEventGenerator != null) {
      watermarkEventGenerator.shutdown();
    }
    windowManager.shutdown();
    bolt.cleanup();
  }
  
  SpearWindowManager<K> getWindowManager() { return windowManager; }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    String lateTupleStream = (String) getComponentConfiguration().get(Config
        .TOPOLOGY_BOLTS_LATE_TUPLE_STREAM);
    if (lateTupleStream != null)
      declarer.declareStream(lateTupleStream, new Fields(LATE_TUPLE_FIELD));
    bolt.declareOutputFields(declarer);
  }
  
  @Override
  public Map<String, Object> getComponentConfiguration() {
    return this.bolt.getComponentConfiguration();
  }
  
  SpearWindowLifecycleListener<K> newWindowLifecycleListener() {
    return new SpearWindowLifecycleListener<K>() {
      
      @Override
      public void onExpiry(List<Tuple> events) {
        for (Tuple t : events)
          windowedOutputCollector.ack(t);
      }
      
      @Override
      public void onActivation(List<Tuple> tuples, List<Tuple> newTuples,
                               List<Tuple> expiredTuples, Long timestamp) {
        windowedOutputCollector.setContext(tuples);
        boltExecute(tuples, newTuples, expiredTuples, timestamp);
      }
      
      @Override
      public void onAcceleratedActivation(List<Tuple> tuples, Long start, Long end,
                                          Map<K, Number> result) {
        windowedOutputCollector.setContext(tuples);
        acceleratedBoltExecute(tuples, start, end, result);
      }
    };
  }
  
  private void boltExecute(List<Tuple> tuples, List<Tuple> newTuples, List<Tuple> expiredTuples,
                           Long timestamp) {
    this.bolt.execute(new TupleWindowImpl(tuples, newTuples, expiredTuples,
        getWindowStartTs(timestamp), timestamp));
  }
  
  protected void acceleratedBoltExecute(List<Tuple> tuples, long windowStart, long windowEnd,
                                        Map<K, Number> result) {
    TupleWindow window = new TupleWindowImpl(tuples, null, null, windowStart,
        windowEnd);
    this.bolt.handleResult(true, window, result);
  }
  
  protected void boltExecute(Supplier<Iterator<Tuple>> tuples, Supplier<Iterator<Tuple>> newTuples,
                             Supplier<Iterator<Tuple>> expiredTuples, Long timestamp) {
    bolt.execute(new TupleWindowIterImpl(tuples, newTuples, expiredTuples,
        getWindowStartTs(timestamp), timestamp));
  }
  
  private Long getWindowStartTs(Long endTs) {
    Long res = null;
    if (endTs != null && windowLengthDuration != null) {
      res = endTs - windowLengthDuration.value;
    }
    return res;
  }
  
  /**
   * Creates an {@link OutputCollector} wrapper that automatically
   * anchors the tuples to inputTuples while emitting.
   */
  private static class WindowedOutputCollector extends OutputCollector {
    private List<Tuple> inputTuples;
    
    WindowedOutputCollector(IOutputCollector delegate) {
      super(delegate);
    }
    
    void setContext(List<Tuple> inputTuples) {
      this.inputTuples = inputTuples;
    }
    
    @Override
    public List<Integer> emit(String streamId, List<Object> tuple) {
      return emit(streamId, inputTuples, tuple);
    }
    
    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
      emitDirect(taskId, streamId, inputTuples, tuple);
    }
  }
}
