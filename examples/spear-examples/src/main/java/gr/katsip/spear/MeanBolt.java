package gr.katsip.spear;

import org.apache.storm.spear.SpearScalarBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.Map;

public class MeanBolt extends SpearScalarBolt {
  
  private OutputCollector collector;
  
  public MeanBolt() { super(); }
  
  @Override
  public void prepare(Map<String, Object> conf, TopologyContext context,
                      OutputCollector collector) {
    super.prepare(conf, context, collector);
    this.collector = collector;
  }
  
  @Override
  public Number produceResult(TupleWindow inputWindow) {
    if (inputWindow.get().size() > 0) {
      double count = 0;
      double sum = 0L;
      for (Tuple t : inputWindow.get()) {
        sum += t.getDoubleByField("value");
        ++count;
      }
      return sum / count;
    }
    return Double.NaN;
  }
  
  @Override
  public void handleResult(boolean acceleration, TupleWindow window, Number result) {
    if (window == null)
      throw new RuntimeException("Window provided is null");
    if (window.getStartTimestamp() == null)
      throw new RuntimeException("window start is null");
    if (window.getEndTimestamp() == null)
      throw new RuntimeException("window end is null");
    if (result == null)
      throw new RuntimeException("result is null");
    if (window.get().size() > 0) {
      ArrayList<Object> out = new ArrayList<>(4);
      out.add(window.getStartTimestamp());
      out.add(window.getEndTimestamp());
      out.add(result.doubleValue());
      out.add(acceleration);
      collector.emit(out);
    }
  }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("start", "end", "mean_payload", "expedited_flag"));
  }
}
