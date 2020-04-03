package gr.katsip.spear;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.storm.spear.SpearBolt;
import org.apache.storm.spear.window.ReservoirSample;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SpearGroupMeanBolt extends SpearBolt<Integer> {
  
  private static final Logger LOG = LoggerFactory.getLogger(SpearGroupMeanBolt.class);
  
  private double zp;
  
  private OutputCollector collector;
  
  public SpearGroupMeanBolt(final float confidence) {
    super(ErrorType.MEAN_ERROR);
    NormalDistribution standardNormal = new NormalDistribution(0.0f, 1.0f);
    this.zp = standardNormal.inverseCumulativeProbability(confidence);
  }
  
  @Override
  public void prepare(Map<String, Object> conf, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
  }
  
  @Override
  public void execute(TupleWindow inputWindow) {
    Map<Integer, Number> result = produceResult(inputWindow);
    handleResult(false, inputWindow, result);
  }
  
  @Override
  public Map<Integer, Number> produceResult(TupleWindow inputWindow) {
    if (inputWindow.get().size() > 0) {
      Map<Integer, Double> sum = new HashMap<>();
      Map<Integer, Integer> histogram = new HashMap<>();
      for (Tuple t : inputWindow.get()) {
        Integer key = t.getIntegerByField("key");
        Double value = t.getDoubleByField("value");
        sum.compute(key, (k, v) -> v == null ? value : v + value);
        histogram.compute(key, (k, v) -> v == null ? 1 : v + 1);
      }
      Map<Integer, Number> result = new HashMap<>(sum.size());
      for (Map.Entry<Integer, Double> e : sum.entrySet()) {
        result.put(e.getKey(), e.getValue() / histogram.get(e.getKey()));
      }
      return result;
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
    SummaryStatistics stats = new SummaryStatistics();
    for (Number n : sample.getSample()) {
      stats.addValue(n.doubleValue());
    }
    result.put(group, stats.getMean());
    if (sample.getSample().length >= sample.getCount()) {
      return 0.0f;
    } else if (sample.getSample().length > 1) {
      double mean = stats.getMean();
      double ci = (zp * stats.getStandardDeviation()) / Math.sqrt(stats.getN());
      double upperError = Math.abs(mean - (mean + ci)) / mean;
      if (mean == 0.0f) {
        upperError = ci;
      }
      return upperError;
    } else {
      return 0.0f;
    }
  }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("start", "end", "result"));
  }
}
