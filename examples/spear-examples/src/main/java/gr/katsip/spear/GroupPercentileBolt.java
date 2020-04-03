package gr.katsip.spear;

import org.apache.storm.spear.SpearBolt;
import org.apache.storm.spear.window.ReservoirSample;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

public class GroupPercentileBolt extends SpearBolt<Integer> {
  
  private static final Logger LOG = LoggerFactory.getLogger(GroupPercentileBolt.class);
  
  private OutputCollector collector;
  
  private final float percentile;
  
  private float confidence;
  
  private float error;
  
  public GroupPercentileBolt(final float percentile, final float error,
                             final float confidence) {
    super(ErrorType.MEAN_ERROR);
    this.percentile = percentile;
    this.error = error;
    this.confidence = confidence;
  }
  
  @Override
  public void prepare(Map<String, Object> conf, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
  }
  
  @Override
  public Map<Integer, Number> produceResult(TupleWindow window) {
    if (window.get().size() > 0) {
    
    }
    return null;
  }
  
  @Override
  public void handleResult(boolean acceleration, TupleWindow window, Map<Integer, Number> result) {
    ArrayList<Object> out = new ArrayList<>(3);
    out.add(window.getStartTimestamp());
    out.add(window.getEndTimestamp());
    ArrayList<String> tokens = new ArrayList<>(result.size());
    for (Map.Entry<Integer, Number> e : result.entrySet()) {
      String token = String.join(":", e.getKey().toString(), e.getValue().toString());
      tokens.add(token);
    }
    out.add(String.join(",", tokens));
    collector.emit(out);
  }
  
  @Override
  public double estimateGroupError(Map<Integer, Number> result, final Integer group,
                                   final ReservoirSample sample) {
    int intError = (int) (100 * error);
    int intConfidence = (int) (100 * confidence);
    ApproxSampleQuantile quantile = new ApproxSampleQuantile(intError, intConfidence);
    if (sample.getSample().length < quantile.getTotalSize()) {
      return 1.0f;
    }
    quantile.load(sample.getSample(), sample.getSampleIndex());
    int percentileValue = quantile.output(percentile);
    if (percentileValue != Integer.MIN_VALUE) {
      result.put(group, (float) percentileValue);
    } else {
      result.put(group, Float.NaN);
    }
    return 0.0f;
  }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("start", "end", "result"));
  }
}
