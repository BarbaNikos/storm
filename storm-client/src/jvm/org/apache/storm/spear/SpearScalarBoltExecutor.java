package org.apache.storm.spear;

import org.apache.storm.Config;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IWindowedBolt;
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

public class SpearScalarBoltExecutor implements IRichBolt {
  
  public static final String LATE_TUPLE_FIELD = "late_tuple";
  private static final Logger LOG = LoggerFactory.getLogger(SpearScalarBoltExecutor.class);
  private static final int DEFAULT_WATERMARK_EVENT_INTERVAL_MS = 1000;
  private static final int DEFAULT_MAX_LAG_MS = 0;
  private final IWindowedBolt bolt;
   transient SpearWatermarkEventGenerator waterMarkEventGenerator;
   private transient WindowedOutputCollector windowedOutputCollector;
   private transient ScalarSpearWindowLifecycleListener listener;
  private transient SpearScalarWindowManager windowManager;
  private transient int maxLagMs;
  private TimestampExtractor timestampExtractor;
  private transient String lateTupleStream;
  private transient TriggerPolicy<Tuple, ?> triggerPolicy;
  private transient EvictionPolicy<Tuple, ?> evictionPolicy;
  private transient BaseWindowedBolt.Duration windowLengthDuration;
  
  private final Function<Object, Number> fieldExtractor;
  private final Aggregate aggregation;
  private int budget;
  private final float error;
  private final float confidence;
  
  public SpearScalarBoltExecutor(IWindowedBolt bolt, Aggregate aggregation,
                                 Function<Object, Number> extractor, int budget,
                                 float error, float confidence) {
    if (!(bolt instanceof SpearScalarBolt)) {
      throw new IllegalArgumentException(String.format("required %s bolt type",
          SpearScalarBolt.class.getSimpleName()));
    }
    this.aggregation = aggregation;
    this.fieldExtractor = extractor;
    this.budget = budget;
    this.error = error;
    this.confidence = confidence;
    this.bolt = bolt;
    this.timestampExtractor = bolt.getTimestampExtractor();
  }
  
  private SpearScalarWindowManager initWindowManager(ScalarSpearWindowLifecycleListener lifecycleListener,
      Map<String, Object> conf, TopologyContext context, Collection<Event<Tuple>> queue) {
    SpearScalarWindowManager manager = new SpearScalarWindowManager(lifecycleListener,
        queue, aggregation, fieldExtractor, budget, error, confidence);
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
      waterMarkEventGenerator = new SpearWatermarkEventGenerator(manager, watermarkInterval, maxLagMs,
          BoltExecutorUtils.getComponentStreams(context));
    }
    BoltExecutorUtils.validate(conf, windowLengthCount, windowLengthDuration,
        slidingIntervalCount, slidingIntervalDuration);
    evictionPolicy = getEvictionPolicy(windowLengthCount, windowLengthDuration);
    triggerPolicy = getTriggerPolicy(slidingIntervalCount, slidingIntervalDuration, manager,
        evictionPolicy);
    manager.setWindowLengthDuration(windowLengthDuration);
    if (windowLengthDuration != null && slidingIntervalDuration != null) {
      manager.setSlidingWindowParameters(windowLengthDuration.value, slidingIntervalDuration.value);
    }
    manager.setEvictionPolicy(evictionPolicy);
    manager.setTriggerPolicy(triggerPolicy);
    return manager;
  }
  
  protected void restoreState(Map<String, Optional<?>> state) {
    windowManager.restoreState(state);
  }
  
  protected Map<String, Optional<?>> getState() {
    return windowManager.getState();
  }
  
  protected void start() {
    if (waterMarkEventGenerator != null) {
      LOG.debug("Starting watermark event generator.");
      waterMarkEventGenerator.start();
    }
    LOG.debug("Starting trigger policy.");
    triggerPolicy.start();
  }
  
  private boolean isTupleTs() { return timestampExtractor != null; }
  
  TriggerPolicy<Tuple, ?> getTriggerPolicy(BaseWindowedBolt.Count slidingIntervalCount,
                                           BaseWindowedBolt.Duration slidingIntervalDuration,
                                           SpearScalarWindowManager manager,
                                           EvictionPolicy<Tuple, ?> evictionPolicy) {
    if (slidingIntervalCount != null) {
      if (isTupleTs()) {
        return new SpearCountTriggerPolicy<>(slidingIntervalCount.value, manager,
            evictionPolicy, manager);
      }
      return new CountTriggerPolicy<>(slidingIntervalCount.value, manager, evictionPolicy);
    } else {
      if (isTupleTs()) {
        return new SpearWatermarkTimeTriggerPolicy<>(slidingIntervalDuration.value, manager,
            evictionPolicy, manager);
      }
      return new TimeTriggerPolicy<>(slidingIntervalDuration.value, manager, evictionPolicy);
    }
  }
  
  EvictionPolicy<Tuple, ?> getEvictionPolicy(BaseWindowedBolt.Count windowLengthCount,
                                             BaseWindowedBolt.Duration windowLengthDuration) {
    if (windowLengthCount != null) {
      if (isTupleTs()) {
        return new WatermarkCountEvictionPolicy<>(windowLengthCount.value);
      }
      return new CountEvictionPolicy<>(windowLengthCount.value);
    } else {
      if (isTupleTs()) {
        return new WatermarkTimeEvictionPolicy<>(windowLengthDuration.value, maxLagMs);
      }
      return new TimeEvictionPolicy<>(windowLengthDuration.value);
    }
  }
  
  @Override
  public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
    doPrepare(conf, context, collector, new ConcurrentLinkedQueue<>());
  }
  
  protected void doPrepare(Map<String, Object> conf, TopologyContext context,
                           OutputCollector collector, Collection<Event<Tuple>> queue) {
    Objects.requireNonNull(conf);
    Objects.requireNonNull(context);
    Objects.requireNonNull(collector);
    Objects.requireNonNull(queue);
    this.windowedOutputCollector = new WindowedOutputCollector(collector);
    this.bolt.prepare(conf, context, windowedOutputCollector);
    this.listener = newWindowLifecycleListener();
    this.windowManager = initWindowManager(listener, conf, context, queue);
    start();
    LOG.info("Intialized approx-scalar-window manager {} ",
        windowManager.getClass().getSimpleName());
  }
  
  @Override
  public void execute(Tuple input) {
    if (isTupleTs()) {
      long ts = timestampExtractor.extractTimestamp(input);
      if (waterMarkEventGenerator.track(input.getSourceGlobalStreamId(), ts)) {
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
    if (waterMarkEventGenerator != null) {
      waterMarkEventGenerator.shutdown();
    }
    windowManager.shutdown();
    bolt.cleanup();
  }
  
  ISpearWindowManager getWindowManager() { return windowManager; }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    String lateTupleStream = (String) getComponentConfiguration()
        .get(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM);
    if (lateTupleStream != null) {
      declarer.declareStream(lateTupleStream, new Fields(LATE_TUPLE_FIELD));
    }
    bolt.declareOutputFields(declarer);
  }
  
  @Override
  public Map<String, Object> getComponentConfiguration() {
    return this.bolt.getComponentConfiguration();
  }
  
  ScalarSpearWindowLifecycleListener newWindowLifecycleListener() {
    return new ScalarSpearWindowLifecycleListener() {
      
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
      public void onExpeditedActivation(List<Tuple> tuples, Long startTime,
                                          Long endTime, Number result) {
        windowedOutputCollector.setContext(tuples);
        acceleratedBoltExecute(tuples, startTime, endTime, result);
      }
    };
  }
  
  protected void boltExecute(List<Tuple> tuples, List<Tuple> newTuples, List<Tuple> expiredTuples,
                             Long timestamp) {
    this.bolt.execute(new TupleWindowImpl(tuples, newTuples, expiredTuples, getWindowStartTs(timestamp), timestamp));
  }
  
  protected void acceleratedBoltExecute(List<Tuple> tuples, long windowStart,
                                        long windowEnd, Number result) {
    ((SpearScalarBolt) this.bolt).handleResult(true, new TupleWindowImpl(tuples, null, null,
        windowStart, windowEnd), result);
  }
  
  protected void boltExecute(Supplier<Iterator<Tuple>> tuples, Supplier<Iterator<Tuple>> newTuples,
                             Supplier<Iterator<Tuple>> expiredTuples, Long timestamp) {
    bolt.execute(new TupleWindowIterImpl(tuples, newTuples, expiredTuples, getWindowStartTs(timestamp), timestamp));
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
    
    WindowedOutputCollector(IOutputCollector delegate) { super(delegate); }
    
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
