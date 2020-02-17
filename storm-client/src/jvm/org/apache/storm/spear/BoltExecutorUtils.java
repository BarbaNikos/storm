package org.apache.storm.spear;

import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.spout.CheckpointSpout;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.windowing.TimestampExtractor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Nikos R. Katsipoulakis (nick.katsip@gmail.com)
 */
public class BoltExecutorUtils {
  
  static int getTopologyTimeoutMillis(final Map<String, Object> conf) {
    if (conf.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS) != null) {
      boolean enabled = (boolean) conf.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS);
      if (!enabled) {
        return Integer.MAX_VALUE;
      }
    }
    int timeout = 0;
    if (conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS) != null)
      timeout = ((Number) conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
    return timeout * 1000;
  }
  
  static int getMaxSpoutPending(final Map<String, Object> conf) {
    int maxPending = Integer.MAX_VALUE;
    if (conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING) != null)
      maxPending = ((Number) conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING)).intValue();
    return maxPending;
  }
  
  static void ensureDurationLessThanTimeout(final int duration, final int timeout) {
    if (duration > timeout)
      throw new IllegalArgumentException("Window duration (length + sliding interval) value " +
          duration + " is more than " + Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS + " value " + timeout);
  }
  
  static void ensureCountLessThanMaxPending(final int count, final int maxPending) {
    if (count > maxPending)
      throw new IllegalArgumentException("Windou count (length + sliding interval) value " +
          count + " is more than " + Config.TOPOLOGY_MAX_SPOUT_PENDING + " value " + maxPending);
  }
  
  public static void validate(final Map<String, Object> conf,
                          final Count windowLengthCount,
                          final Duration windowLengthDuration,
                          final Count slidingIntervalCount,
                          final Duration slidingIntervalDuration) {
    int topologyTimeout = BoltExecutorUtils.getTopologyTimeoutMillis(conf);
    int maxSpoutPending = BoltExecutorUtils.getMaxSpoutPending(conf);
    if (windowLengthCount == null && windowLengthDuration == null) {
      throw new IllegalArgumentException("window length is not specified");
    }
    if (windowLengthDuration != null && slidingIntervalDuration != null) {
      ensureDurationLessThanTimeout(windowLengthDuration.value + slidingIntervalDuration.value,
          topologyTimeout);
    } else if (windowLengthDuration != null) {
      ensureDurationLessThanTimeout(windowLengthDuration.value, topologyTimeout);
    } else if (slidingIntervalDuration != null) {
      ensureDurationLessThanTimeout(slidingIntervalDuration.value, maxSpoutPending);
    }
    if (windowLengthCount != null && slidingIntervalCount != null) {
      ensureCountLessThanMaxPending(windowLengthCount.value + slidingIntervalCount.value,
          maxSpoutPending);
    } else if (windowLengthCount != null) {
      ensureCountLessThanMaxPending(windowLengthCount.value, maxSpoutPending);
    } else if (slidingIntervalCount != null) {
      ensureCountLessThanMaxPending(slidingIntervalCount.value, maxSpoutPending);
    }
  }
  
  static Count getWindowLengthCount(final Map<String, Object> conf) {
    if (conf.containsKey(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT))
      return new Count(((Number) conf.get(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT)).intValue());
    return null;
  }
  
  static Duration getWindowLengthDuration(final Map<String, Object> conf) {
    if (conf.containsKey(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS))
      return new Duration(((Number) conf.get(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS))
          .intValue(), TimeUnit.MILLISECONDS);
    return null;
  }
  
  static Count getSlidingIntervalCount(final Map<String, Object> conf) {
    if (conf.containsKey(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT))
      return new Count(((Number) conf.get(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT))
          .intValue());
    return null;
  }
  
  static Duration getSlidingIntervalDuration(final Map<String, Object> conf) {
    if (conf.containsKey(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS))
      return new Duration(((Number) conf.get(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS))
          .intValue(), TimeUnit.MILLISECONDS);
    return null;
  }
  
  static void validateLateTupleStream(final TimestampExtractor extractor,
                                   final Map<String, Object> conf,
                                   final TopologyContext context) {
    if (extractor != null) {
      String lateTupleStream = (String) conf.get(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM);
      if (lateTupleStream != null) {
        if (!context.getThisStreams().contains(lateTupleStream))
          throw new IllegalArgumentException("Stream for late tuples must be defined with " +
              "the builder method withLateTupleStream()");
      }
    } else {
      if (conf.containsKey(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM))
        throw new IllegalArgumentException("Late tuple stream can be defined only " + "" +
            "when specifying a timestamp field");
    }
  }
  
  static int getMaximumLagInMillis(final TimestampExtractor extractor,
                                   final Map<String, Object> conf) {
    if (extractor != null) {
      if (conf.containsKey(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS)) {
        return ((Number) conf.get(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS)).intValue();
      }
    }
    return 1000;
  }
  
  static int getWatermarkIntervalInMillis(final TimestampExtractor extractor,
                                  final Map<String, Object> conf) {
    if (extractor != null) {
      if (conf.containsKey(Config.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS))
        return ((Number) conf.get(Config.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS)).intValue();
    }
    return 1000;
  }
  
  static Set<GlobalStreamId> getComponentStreams(TopologyContext context) {
    Set<GlobalStreamId> streams = new HashSet<>();
    for (GlobalStreamId streamId : context.getThisSources().keySet())
      if (!streamId.get_streamId().equals(CheckpointSpout.CHECKPOINT_STREAM_ID))
        streams.add(streamId);
    return streams;
  }
}
