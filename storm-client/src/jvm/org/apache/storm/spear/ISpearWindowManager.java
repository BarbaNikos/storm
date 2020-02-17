package org.apache.storm.spear;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.Event;

import java.util.List;

public interface ISpearWindowManager {
  int getEventCount(long referenceTime);
  List<Long> getSlidingWindowCountTimestamps(long startTs, long endTs, int slidingCount);
  void add(Event<Tuple> event);
  long getEarliestEventTs(long startTs, long endTs);
}
