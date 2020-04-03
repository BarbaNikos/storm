package org.apache.storm.spear;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.WindowLifecycleListener;

import java.util.List;
import java.util.Map;

public interface SpearWindowLifecycleListener<K> extends WindowLifecycleListener<Tuple> {
  
  default void onAcceleratedActivation(List<Tuple> tuples, Long start, Long end, Map<K, Number> result) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
