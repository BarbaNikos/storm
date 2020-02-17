package org.apache.storm.spear;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.WindowLifecycleListener;

import java.util.List;

public interface ScalarSpearWindowLifecycleListener extends WindowLifecycleListener<Tuple> {
  default void onExpeditedActivation(List<Tuple> events, Long startTime, Long endTime,
                                     Number result) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
