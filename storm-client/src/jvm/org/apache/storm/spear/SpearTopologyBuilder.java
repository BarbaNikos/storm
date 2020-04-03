package org.apache.storm.spear;

import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.TopologyBuilder;

import java.util.function.Function;

/**
 * SpearTopologyBuilder exposes the SPEAR API for building topologies with the Java API. The main
 * difference compared to its parent {@link org.apache.storm.topology.TopologyBuilder} is that
 * this class allows definition of stateful operations with an approximation specification.
 */
public class SpearTopologyBuilder extends TopologyBuilder {
  
  public BoltDeclarer setApproxScalarBolt(String id, IWindowedBolt bolt, int budget,
                                          ScalarAggregate aggregation,
                                          Function<Object, Number> fieldExtractor,
                                          float error,
                                          float confidence,
                                          Number parallelismHint) {
    return setBolt(id, new SpearScalarBoltExecutor(bolt, aggregation, fieldExtractor, budget,
        error, confidence), parallelismHint);
  }
  
  public <K> BoltDeclarer setApproxBolt(String id, SpearBolt<K> bolt, int budget,
                                        Function<Object, K> keyExtractor,
                                        Function<Object, Number> valueExtractor,
                                        float error,
                                        float confidence,
                                        Number parallelismHint) {
    return setBolt(id, new SpearBoltExecutor<K>(bolt, keyExtractor, valueExtractor, budget, error,
        confidence), parallelismHint);
  }
}
