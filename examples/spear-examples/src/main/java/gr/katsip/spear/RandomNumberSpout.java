package gr.katsip.spear;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class RandomNumberSpout extends BaseRichSpout {
  
  private static final String[] FIELDS = { "time", "value" };
  
  private SpoutOutputCollector outputCollector;
  
  private NormalDistribution distribution;
  
  private long count;
  
  @Override
  public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
    this.outputCollector = collector;
    this.distribution = new NormalDistribution(100.0f, 15.0f);
    count = 0L;
  }
  
  @Override
  public void nextTuple() {
    double sample = distribution.sample();
    outputCollector.emit(new Values(System.currentTimeMillis(), sample), count++);
  }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS));
  }
}
