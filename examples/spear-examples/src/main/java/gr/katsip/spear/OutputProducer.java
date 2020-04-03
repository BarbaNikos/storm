package gr.katsip.spear;

import org.apache.storm.windowing.TupleWindow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.function.BiFunction;

public class OutputProducer implements Serializable, BiFunction<TupleWindow, Number, ArrayList<Object>> {
  @Override
  public ArrayList<Object> apply(TupleWindow window, Number number) {
    ArrayList<Object> tuple = new ArrayList<>(3);
    tuple.add(window.getStartTimestamp());
    tuple.add(window.getEndTimestamp());
    tuple.add(number.intValue());
    return tuple;
  }
}
