package gr.katsip.spear;

import org.apache.storm.Config;
import org.apache.storm.spear.SpearScalarBolt;
import org.apache.storm.spear.SpearTopologyBuilder;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

public class ScalarMedian extends ConfigurableTopology {
  
  public static void main(String[] args) throws Exception {
    ConfigurableTopology.start(new ScalarMedian(), args);
  }
  
  @Override
  protected int run(String[] args) throws Exception {
    float error = .1f;
    float confidence = .95f;
    RandomNumberSpout spout = new RandomNumberSpout();
    SpearScalarBolt bolt = (SpearScalarBolt) (new SpearMedianBolt()
        .withValueExtractor(new ValueExtractor())
        .withOutputProducer(new OutputProducer())
        .withOutputFields(new Fields("start", "end", "median_payload")))
        .withTimestampExtractor(x -> x.getLong(0))
        .withTumblingWindow(BaseWindowedBolt.Duration.of(50));
  
    SpearTopologyBuilder builder = new SpearTopologyBuilder();
    builder.setSpout("numbers", spout, 1);
    builder.setApproxScalarBolt("spear-median", bolt, 100,
        new ApproxQuantileAggregation(),
        new ValueExtractor(),
        error, confidence, 1)
        .shuffleGrouping("numbers");
    Config config = new Config();
    config.put("topology.name", "spear-median-topo");
    return submit("spear-median-topo", config, builder);
  }
}
