package org.apache.storm.spear;

import java.util.List;

public abstract class ScalarAggregate {
  
  protected float sum;
  
  protected float count;
  
  public abstract boolean confidenceIntervalTest(Number[] sample, final int offset,
                                                 final float error,
                                                 final float confidence,
                                                 final int windowSize,
                                                 List<Number> result);
}
