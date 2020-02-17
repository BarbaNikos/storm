package org.apache.storm.spear.window;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class that offers methods for assigning a tuple to a window.
 */
public class WindowAssignUtils {
  
  /**
   * Given a time-based window specification and a timestamp, this method returns the windows in
   * which the given timestamp is part of.
   * @param range the range of the time-based window in milliseconds. This parameter needs to be
   *              a positive long value, representing a window size in milliseconds.
   * @param slide the slide of the time-based window in milliseconds. This parameter needs to be
   *              a positive long value, representing a window range in milliseconds.
   * @param timestamp the timestamp in milliseconds. This parameter needs to be a positive long
   *                  value, representing a Unix epoch (i.e., timestamp) in millisecond accuracy.
   * @return a {@link List<TimeWindow>} which contains the time-based windows in which the given
   * timestamp is part of.
   */
  public static List<TimeWindow> assignWindows(final long range, final long slide,
                                               final long timestamp) {
    if (timestamp > Long.MIN_VALUE) {
      List<TimeWindow> windows = new ArrayList<>((int) (range / slide));
      long lastStart = getWindowStart(timestamp, slide);
      for (long start = lastStart; start > timestamp - range; start -= slide) {
        windows.add(new TimeWindow(start, start + range));
      }
      return windows;
    }
    throw new RuntimeException("Given timestamp has invalid timestamp.");
  }
  
  private static long getWindowStart(long timestamp, long slide) {
    return timestamp - (timestamp + slide) % slide;
  }
}
