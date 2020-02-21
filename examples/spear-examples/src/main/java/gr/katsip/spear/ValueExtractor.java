package gr.katsip.spear;

import org.apache.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.function.Function;

public class ValueExtractor implements Function<Object, Number>, Serializable {
  @Override
  public Number apply(Object o) {
    Tuple t = (Tuple) o;
    return t.getDoubleByField("value");
  }
}
