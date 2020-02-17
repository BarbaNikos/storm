package org.apache.storm.spear.window;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Represents a reservoir-based sample.
 */
public class ReservoirSample {
  
  private final long budget;
  
  private Number[] sample;
  
  private int sampleIndex;
  
  private int attempts;
  
  private long count;
  
  private float sum;
  
  public ReservoirSample(long budget) {
    this.budget = budget;
    this.sample = new Number[(int) this.budget];
    this.sampleIndex = -1;
    this.attempts = 0;
    this.sum = 0.0f;
  }
  
  public void addValue(Number value) {
    if (sampleIndex < sample.length - 1) {
      sample[++sampleIndex] = value;
      sum += value.floatValue();
    } else {
      final int flip = ThreadLocalRandom.current().nextInt(0, attempts + 1);
      if (flip < sample.length) {
        sum = sum - sample[flip].floatValue() + value.floatValue();
        sample[flip] = value;
      }
    }
    attempts += 1;
    count += 1;
  }
  
  public int getSampleIndex() {
    return sampleIndex;
  }
  
  public Number[] getSample() {
    return sample;
  }
  
  public long getCount() {
    return count;
  }
  
  public float getSum() {
    return sum;
  }
  
  public float getStdev() {
    SummaryStatistics stats = new SummaryStatistics();
    for (int i = 0; i <= sampleIndex; ++i) {
      stats.addValue(sample[i].floatValue());
    }
    return (float) stats.getStandardDeviation();
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    
    ReservoirSample that = (ReservoirSample) o;
    
    if (budget != that.budget) return false;
    if (sampleIndex != that.sampleIndex) return false;
    if (attempts != that.attempts) return false;
    if (count != that.count) return false;
    // Probably incorrect - comparing Object[] arrays with Arrays.equals
    return Arrays.equals(sample, that.sample);
  }
  
  @Override
  public int hashCode() {
    int result = (int) (budget ^ (budget >>> 32));
    result = 31 * result + Arrays.hashCode(sample);
    result = 31 * result + sampleIndex;
    result = 31 * result + attempts;
    result = 31 * result + (int) (count ^ (count >>> 32));
    return result;
  }
}
