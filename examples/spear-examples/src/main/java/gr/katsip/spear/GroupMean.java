package gr.katsip.spear;

import org.apache.storm.Config;
import org.apache.storm.spear.SpearBolt;
import org.apache.storm.spear.SpearTopologyBuilder;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

public class GroupMean extends ConfigurableTopology {
  
  public static void main(String[] args) throws Exception {
    ConfigurableTopology.start(new GroupMean(), args);
  }
  
  @Override
  protected int run(String[] args) throws Exception {
    float error = .1f;
    float confidence = .95f;
    SpearGroupMeanBolt bolt = (SpearGroupMeanBolt) (new SpearGroupMeanBolt(confidence))
        .withTimestampExtractor(x -> x.getLong(0))
        .withTumblingWindow(BaseWindowedBolt.Duration.of(100));
    
    BaseWindowedBolt vanillaBolt = new GroupMeanBolt()
        .withTimestampExtractor(x -> x.getLong(0))
        .withTumblingWindow(BaseWindowedBolt.Duration.of(100));
    
    SpearTopologyBuilder builder = new SpearTopologyBuilder();
    builder.setSpout("groups", new RandomGroupNumberSpout(), 1);
//    builder.setBolt("mean", vanillaBolt, 1).shuffleGrouping("groups");
    
    builder.setApproxBolt("spear-mean", bolt, 5, new KeyExtractor(),
        new ValueExtractor(), error, confidence, 1).shuffleGrouping("groups");
//    builder.setBolt("error", new GroupMeanErrorBolt(), 1)
//        .shuffleGrouping("mean")
//        .shuffleGrouping("spear-mean");
    Config config = new Config();
    config.put("topology.name", "spear-group-mean-topo");
    return submit("spear-group-mean-topo", config, builder);
  }
}
