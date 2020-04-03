package gr.katsip.spear;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class GroupMeanBolt extends BaseWindowedBolt {
  
  private static final Logger LOG = LoggerFactory.getLogger(GroupMeanBolt.class);
  
  private OutputCollector collector;
  
  @Override
  public void prepare(Map<String, Object> conf, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
  }
  
  @Override
  public void execute(TupleWindow inputWindow) {
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
      ArrayList<Object> out = new ArrayList<>(3);
      out.add(inputWindow.getStartTimestamp());
      out.add(inputWindow.getEndTimestamp());
      ArrayList<String> tokens = new ArrayList<>(result.size());
      for (Map.Entry<Integer, Number> e : result.entrySet()) {
        String token = String.join(":", e.getKey().toString(), e.getValue().toString());
        tokens.add(token);
      }
      out.add(String.join(",", tokens));
      collector.emit(out);
    }
  }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("start", "end", "result"));
  }
}
