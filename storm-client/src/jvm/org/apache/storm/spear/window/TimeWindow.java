package org.apache.storm.spear.window;

import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;

/**
 * This class represents a time window, which is a pair of timestamps: start and end. Both
 * timestamps represent time up to milliseconds granularity.
 */
public class TimeWindow {
  
  private final long start;
  
  private final long end;
  
  /**
   * Initialize a {@link TimeWindow} object with the given bounds measured in a specific time unit.
   * @param start the start of the window.
   * @param end the end of the window
   * @param timeUnit the {@link TimeUnit} in which start and end are represented.
   */
  public TimeWindow(long start, long end, TimeUnit timeUnit) {
    this(timeUnit.toMillis(start), timeUnit.toMillis(end));
  }
  
  /**
   * Main constructor that initializes the {@link TimeWindow} object with the given bounds. Both
   * bounds are assumed to be representing timestamps in milliseconds.
   * @param start the start of the window.
   * @param end the end of the window.
   */
  public TimeWindow(long start, long end) {
    Preconditions.checkArgument(end > start, "end must be greater than start");
    this.start = start;
    this.end = end;
  }
  
  public long getStart() {
    return start;
  }
  
  public long getEnd() {
    return end;
  }
  
  /**
   * Returns the maximum allowed time in the window, in the order of milliseconds.
   * @return the maximum allowed time in milliseconds in the window.
   */
  public long maxTimestamp() {
    return end - 1;
  }
  
  /**
   * Checks whether another window overlaps with the current window.
   * @param other a {@link TimeWindow} which is checked for overlapping time durations.
   * @return true if the two windows overlap; false otherwise.
   */
  public boolean intersects(TimeWindow other) {
    return this.start <= other.start && this.end >= other.start;
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    
    TimeWindow that = (TimeWindow) o;
    
    if (start != that.start) return false;
    return end == that.end;
  }
  
  @Override
  public int hashCode() {
    int result = (int) (start ^ (start >>> 32));
    result = 31 * result + (int) (end ^ (end >>> 32));
    return result;
  }
}
