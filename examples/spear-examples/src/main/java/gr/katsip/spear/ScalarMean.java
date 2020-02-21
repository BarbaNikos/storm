package gr.katsip.spear;

import org.apache.storm.Config;
import org.apache.storm.spear.SpearScalarBolt;
import org.apache.storm.spear.SpearTopologyBuilder;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;

import java.util.function.Function;

public class ScalarMean extends ConfigurableTopology {
  
  public static void main(String[] args) throws Exception {
    ConfigurableTopology.start(new ScalarMean(), args);
  }
  
  @Override
  protected int run(String[] args) throws Exception {
    float error = .05f;
    float confidence = .95f;
    RandomNumberSpout spout = new RandomNumberSpout();
    SpearScalarBolt bolt = (SpearScalarBolt) (new MeanBolt())
        .withTimestampExtractor(x -> x.getLong(0))
        .withTumblingWindow(BaseWindowedBolt.Duration.of(100));
    SpearTopologyBuilder builder = new SpearTopologyBuilder();
    builder.setSpout("numbers", spout, 1);
    builder.setApproxScalarBolt("spear-mean", bolt, 100, new MeanTest(confidence),
        new ValueExtractor(), error, confidence, 1).shuffleGrouping("numbers");
    Config config = new Config();
    config.put("topology.name", "spear-mean-topo");
    return submit("spear-mean-topo", config, builder);
  }
}
