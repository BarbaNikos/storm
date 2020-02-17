package org.apache.storm.spear;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.topology.FailedException;
import org.apache.storm.windowing.WaterMarkEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class SpearWatermarkEventGenerator implements Runnable {
  
  private static final Logger LOG = LoggerFactory.getLogger(SpearWatermarkEventGenerator.class);
  
  private final ISpearWindowManager windowManager;
  
  private final int eventTsLag;
  
  private final Set<GlobalStreamId> inputStreams;
  
  private final Map<GlobalStreamId, Long> streamToTs;
  
  private final ScheduledExecutorService executorService;
  
  private final int interval;
  
  private ScheduledFuture<?> executorFuture;
  
  private volatile long lastWatermarkTs;
  
  public SpearWatermarkEventGenerator(ISpearWindowManager windowManager, int intervalMs,
                                      int eventTsLagMs, Set<GlobalStreamId> inputStreams) {
    this.windowManager = windowManager;
    streamToTs = new ConcurrentHashMap<>();
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("spear-watermark-event-generator-%d")
        .setDaemon(true)
        .build();
    executorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
    this.interval = intervalMs;
    this.eventTsLag = eventTsLagMs;
    this.inputStreams = inputStreams;
  }
  
  /**
   * Tracks the timestamp of the event in the stream, returns true if the event can be considered
   * for processing or false if its a late event.
   * @param stream
   * @param ts
   * @return
   */
  public boolean track(GlobalStreamId stream, long ts) {
    Long currentVal = streamToTs.get(stream);
    if (currentVal == null || ts > currentVal) {
      streamToTs.put(stream, ts);
    }
    checkFailures();
    return ts >= lastWatermarkTs;
  }
  
  @Override
  public void run() {
    try {
      long watermarkTs = computeWatermarkTs();
      if (watermarkTs > lastWatermarkTs) {
        this.windowManager.add(new WaterMarkEvent<>(watermarkTs));
        lastWatermarkTs = watermarkTs;
      }
    } catch (Throwable t) {
      LOG.error("Failed while processing watermark event ", t);
      throw t;
    }
  }
  
  /**
   * Computes the minimum timestamp across all streams.
   * @return the minimum timestamp across all streams reduced by the allowed lag.
   */
  private long computeWatermarkTs() {
    long ts = 0;
    if (streamToTs.size() >= inputStreams.size()) {
      ts = Long.MAX_VALUE;
      for (Map.Entry<GlobalStreamId, Long> entry : streamToTs.entrySet()) {
        ts = Math.min(ts, entry.getValue());
      }
    }
    return ts - eventTsLag;
  }
  
  private void checkFailures() {
    if (executorFuture != null && executorFuture.isDone()) {
      try {
        executorFuture.get();
      } catch (InterruptedException ex) {
        LOG.error("Got exception ", ex);
        throw new FailedException(ex);
      } catch (ExecutionException ex) {
        LOG.error("Got exception ", ex);
        throw new FailedException(ex.getCause());
      }
    }
  }
  
  public void start() {
    this.executorFuture = executorService.scheduleAtFixedRate(this, interval, interval,
        TimeUnit.MILLISECONDS);
  }
  
  public void shutdown() {
    LOG.debug("Shutting down ShedWaterMarkEventGenerator");
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(2, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException ex) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
