package org.apache.storm.spear;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.*;

import java.util.List;

public class SpearWatermarkCountTriggerPolicy<K> implements TriggerPolicy<Tuple, Long> {
  
  private final int count;
  
  private final TriggerHandler handler;
  
  private final EvictionPolicy<Tuple, ?> evictionPolicy;
  
  private final SpearWindowManager<K> windowManager;
  
  private volatile long lastProcessedTs;
  
  private boolean started;
  
  public SpearWatermarkCountTriggerPolicy(int count, TriggerHandler handler,
                                           EvictionPolicy<Tuple, ?> evictionPolicy,
                                           SpearWindowManager<K> windowManager) {
    this.count = count;
    this.handler = handler;
    this.evictionPolicy = evictionPolicy;
    this.windowManager = windowManager;
    this.started = false;
  }
  
  @Override
  public void track(Event<Tuple> event) {
    if (started && event.isWatermark())
      handleWaterMarkEvent(event);
  }
  
  @Override
  public void reset() {
    // No-OP
  }
  
  @Override
  public void start() {
    started = true;
  }
  
  @Override
  public void shutdown() {
    // No-OP
  }
  
  /**
   * Triggers all the pending windows up to the watermark event timestamp based on the
   * sliding interval count.
   * @param waterMarkEvent the watermark event
   */
  private void handleWaterMarkEvent(Event<Tuple> waterMarkEvent) {
    long watermarkTs = waterMarkEvent.getTimestamp();
    List<Long> eventTs = windowManager.getSlidingWindowCountTimestamps(lastProcessedTs, watermarkTs,
        count);
    for (long t : eventTs) {
      evictionPolicy.setContext(new DefaultEvictionContext(t, null, Long.valueOf(count)));
      handler.onTrigger();
      lastProcessedTs = t;
    }
  }
  
  @Override
  public Long getState() {
    return lastProcessedTs;
  }
  
  @Override
  public void restoreState(Long state) {
    lastProcessedTs = state;
  }
  
  @Override
  public String toString() {
    return "SpearWatermarkCountTriggerPolicy{" + "count=" + count +
        ", lastProcessedTs=" + lastProcessedTs + ", started=" + started + '}';
  }
}
