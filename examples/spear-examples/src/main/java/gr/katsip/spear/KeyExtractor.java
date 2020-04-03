package gr.katsip.spear;

import org.apache.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.function.Function;

public class KeyExtractor implements Function<Object, Integer>, Serializable {
  @Override
  public Integer apply(Object o) {
    Tuple t = (Tuple) o;
    return t.getIntegerByField("key");
  }
}
