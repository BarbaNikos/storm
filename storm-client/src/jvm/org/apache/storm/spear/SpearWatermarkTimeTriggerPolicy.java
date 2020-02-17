package org.apache.storm.spear;

import org.apache.storm.windowing.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpearWatermarkTimeTriggerPolicy<T> implements TriggerPolicy<T, Long> {
  
  private static final Logger LOG = LoggerFactory.getLogger(SpearWatermarkTimeTriggerPolicy.class);
  
  private final long slidingIntervalMs;
  
  private final TriggerHandler handler;
  
  private final EvictionPolicy<T, ?> evictionPolicy;
  
  private final ISpearWindowManager windowManager;
  
  private volatile long nextWindowEndTs;
  
  private boolean started;
  
  public SpearWatermarkTimeTriggerPolicy(long slidingIntervalMs, TriggerHandler handler,
                                         EvictionPolicy<T, ?> evictionPolicy,
                                         ISpearWindowManager windowManager) {
    this.slidingIntervalMs = slidingIntervalMs;
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
  public void start() {
    started = true;
  }
  
  @Override
  public void shutdown() {}
  
  /**
   * Invokes the trigger all pending windows up to the watermark timestamp. The end timestamp of
   * the window is set in the eviction policy context so that the events falling withing that
   * window can be processed.
   * @param event
   */
  private void handleWaterMarkEvent(Event<T> event) {
    long watermarkTs = event.getTimestamp();
    long windowEndTs = nextWindowEndTs;
    LOG.debug("Window end ts {} Watermark ts {}", windowEndTs, watermarkTs);
    while (windowEndTs <= watermarkTs) {
      long currentCount = windowManager.getEventCount(windowEndTs);
      evictionPolicy.setContext(new DefaultEvictionContext(windowEndTs, currentCount));
      if (handler.onTrigger()) {
        windowEndTs += slidingIntervalMs;
      } else {
        long ts = getNextAlignedWindowTs(windowEndTs, watermarkTs);
        LOG.debug("Next aligned window end ts {}", ts);
        if (ts == Long.MAX_VALUE) {
          LOG.debug("No events to process between {} and watermark ts {}", windowEndTs, watermarkTs);
          break;
        }
        windowEndTs = ts;
      }
    }
    nextWindowEndTs = windowEndTs;
  }
  
  /**
   * Computes the next window by scanning the events in the window and finds the next aligned
   * window between the startTs and endTs. Return the end timestamp of the next aligned window (i
   * .e., the timestamp when the window should fire).
   * @param startTs the start timestamp (excluding)
   * @param endTs the end timestamp (including)
   * @return the aligned window end timestamp for the next window or Long.MAX_VALUE if there are
   * no more events to be processed.
   */
  private long getNextAlignedWindowTs(long startTs, long endTs) {
    long nextTs = windowManager.getEarliestEventTs(startTs, endTs);
    if (nextTs == Long.MAX_VALUE || (nextTs % slidingIntervalMs == 0)) {
      return nextTs;
    }
    return nextTs + (slidingIntervalMs - (nextTs % slidingIntervalMs));
  }
  
  @Override
  public Long getState() {
    return nextWindowEndTs;
  }
  
  @Override
  public void restoreState(Long state) {
    nextWindowEndTs = state;
  }
  
  @Override
  public String toString() {
    return "SpearWatermarkTimeTriggerPolicy{" +
        "slidingIntervalMs=" + slidingIntervalMs +
        ", nextWindowEndTs=" + nextWindowEndTs +
        ", started=" + started +
        '}';
  }
}
