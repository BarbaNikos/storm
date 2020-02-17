package org.apache.storm.spear;

import org.apache.storm.windowing.*;

import java.util.List;

public class SpearCountTriggerPolicy<T> implements TriggerPolicy<T, Long> {
  private final int count;
  
  private final TriggerHandler handler;
  
  private final EvictionPolicy<T, ?> evictionPolicy;
  
  private final ISpearWindowManager windowManager;
  
  private volatile long lastProcessedTs;
  
  private boolean started;
  
  public SpearCountTriggerPolicy(int count, TriggerHandler handler,
                                 EvictionPolicy<T, ?> evictionPolicy,
                                 ISpearWindowManager windowManager) {
    this.count = count;
    this.handler = handler;
    this.evictionPolicy = evictionPolicy;
    this.windowManager = windowManager;
    this.started = false;
  }
  
  @Override
  public void track(Event<T> event) {
    if (started && event.isWatermark()) {
      handleWaterMarkEvent(event);
    }
  }
  
  @Override
  public void reset() {}
  
  @Override
  public void start() { started = true; }
  
  @Override
  public void shutdown() {}
  
  /**
   * Triggers all the pending windows up to the watermark event timestamp based on the
   * sliding interval count.
   * @param waterMarkEvent the watermark event
   */
  private void handleWaterMarkEvent(Event<T> waterMarkEvent) {
    long watermarkTs = waterMarkEvent.getTimestamp();
    List<Long> eventTs = windowManager.getSlidingWindowCountTimestamps(lastProcessedTs, watermarkTs,
        count);
    for (long t : eventTs) {
      evictionPolicy.setContext(new DefaultEvictionContext(t, null, (long) count));
      handler.onTrigger();
      lastProcessedTs = t;
    }
  }
  
  @Override
  public Long getState() { return lastProcessedTs; }
  
  @Override
  public void restoreState(Long state) { lastProcessedTs = state; }
  
  @Override
  public String toString() {
    return "SpearCountTriggerPolicy{" + "count=" + count +
        ", lastProcessedTs=" + lastProcessedTs + ", started=" + started + '}';
  }
}
