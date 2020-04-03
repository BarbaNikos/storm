package gr.katsip.spear;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class RandomGroupNumberSpout extends BaseRichSpout {
  
  private static final Logger LOG = LoggerFactory.getLogger(RandomGroupNumberSpout.class);
  
  private static final String[] FIELDS = {"time", "key", "value" };
  
  private SpoutOutputCollector outputCollector;
  
  private NormalDistribution distribution;
  
  private long count;
  
  @Override
  public void open(Map<String, Object> conf, TopologyContext context,
                   SpoutOutputCollector collector) {
    this.outputCollector = collector;
    this.distribution = new NormalDistribution(100.0f, 15.0f);
    count = 0L;
  }
  
  @Override
  public void nextTuple() {
    double sample = (float) distribution.sample();
    outputCollector.emit(new Values(System.currentTimeMillis(),
        ThreadLocalRandom.current().nextInt(5), sample), count++);
  }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(FIELDS));
  }
}
