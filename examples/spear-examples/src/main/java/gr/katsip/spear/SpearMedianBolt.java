package gr.katsip.spear;

import org.apache.storm.spear.SpearScalarBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

public class SpearMedianBolt extends SpearScalarBolt {
  
  private static Logger LOG = LoggerFactory.getLogger(SpearMedianBolt.class);
  
  private OutputCollector collector;
  
  private Function<Object, Number> valueExtractor;
  
  private BiFunction<TupleWindow, Number, ArrayList<Object>> shapeOutput;
  
  private Fields outputFields;
  
  @Override
  public void prepare(Map<String, Object> conf, TopologyContext context,
                      OutputCollector collector) {
    super.prepare(conf, context, collector);
    if (valueExtractor == null) {
      throw new RuntimeException("value extractor is null.");
    }
    if (shapeOutput == null) {
      throw new RuntimeException("output producer is null.");
    }
    this.collector = collector;
  }
  
  @Override
  public Number produceResult(TupleWindow window) {
    int size = window.get().size();
    List<Tuple> tuples = window.get();
    if (window.get().size() > 0) {
      Collections.sort(tuples,
          Comparator.comparing(t -> valueExtractor.apply(t).intValue()));
      if (size % 2 == 0) {
        int middle = (int) Math.floor(size / 2);
        int one = valueExtractor.apply(tuples.get(middle)).intValue();
        int two = valueExtractor.apply(tuples.get(middle + 1)).intValue();
        return (one + two) / 2;
      }
      return valueExtractor.apply(tuples.get(size / 2)).intValue();
    }
    return Float.NaN;
  }
  
  @Override
  public void handleResult(boolean approximate, TupleWindow window, Number result) {
    collector.emit(shapeOutput.apply(window, result));
  }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    if (outputFields == null) {
      throw new RuntimeException("output fields are not set (null).");
    }
    declarer.declare(outputFields);
  }
  
  public SpearMedianBolt withValueExtractor(Function<Object, Number> valueExtractor) {
    this.valueExtractor = valueExtractor;
    return this;
  }
  
  public SpearMedianBolt withOutputProducer(BiFunction<TupleWindow, Number, ArrayList<Object>> outputProducer) {
    this.shapeOutput = outputProducer;
    return this;
  }
  
  public SpearMedianBolt withOutputFields(Fields outputFields) {
    this.outputFields = outputFields;
    return this;
  }
}
