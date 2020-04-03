package gr.katsip.spear;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class GroupMeanErrorBolt extends BaseRichBolt {
  
  private static final Logger LOG = LoggerFactory.getLogger(GroupMeanErrorBolt.class);
  
  private Map<String, Map<Integer, Number>> results;
  
  private Map<String, Map<Integer, Number>> spearResults;
  
  private OutputCollector collector;
  
  private LinkedList<String> log;
  
  @Override
  public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    this.results = new HashMap<>();
    this.spearResults = new HashMap<>();
    log = new LinkedList<>();
  }
  
  @Override
  public void execute(Tuple input) {
    String sourceComponent = input.getSourceComponent();
    String key = String.format("%d-%d", input.getLongByField("start"), input.getLongByField("end"));
    Map<Integer, Number> result = deserializeValues(input.getStringByField("result"));
    if (sourceComponent.contains("spear")) {
      if (results.containsKey(key)) {
        log.add(error(results.get(key), result));
        results.remove(key);
      } else {
        spearResults.put(key, result);
      }
    } else {
      if (spearResults.containsKey(key)) {
        log.add(error(spearResults.get(key), result));
        spearResults.remove(key);
      } else {
        results.put(key, result);
      }
    }
  }
  
  private String error(Map<Integer, Number> base, Map<Integer, Number> estimate) {
    SummaryStatistics errors = new SummaryStatistics();
    for (Map.Entry<Integer, Number> e : base.entrySet()) {
      if (estimate.containsKey(e.getKey())) {
        double relError = Math.abs(e.getValue().floatValue() -
            estimate.get(e.getKey()).floatValue()) / Math.max(e.getValue().floatValue(), estimate
            .get(e.getKey()).floatValue());
        errors.addValue(relError);
      } else {
        errors.addValue(1.0f);
      }
    }
    return String.format("%f-%f", errors.getMean(), errors.getMax());
  }
  
  private Map<Integer, Number> deserializeValues(String value) {
    String[] tokens = value.split(",");
    HashMap<Integer, Number> result = new HashMap<>(tokens.length);
    for (String token : tokens) {
      Integer group = Integer.parseInt(token.split(":")[0]);
      Number val = Float.parseFloat(token.split(":")[1]);
      result.put(group, val);
    }
    return result;
  }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  
  }
  
  @Override
  public void cleanup() {
    LOG.info(String.format("Concluded execution with %d windows", log.size()));
    for (String s : log) {
      LOG.info(s);
    }
    LOG.info("+++ DONE +++");
  }
}
