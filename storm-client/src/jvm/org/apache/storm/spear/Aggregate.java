package org.apache.storm.spear;

import java.util.List;

public abstract class Aggregate {
  
  protected float sum;
  
  protected float stdev;
  
  protected float count;
  
//  public void setSum(float sum) {
//    this.sum = sum;
//  }
//
//  public void setStdev(float stdev) {
//    this.stdev = stdev;
//  }
//
//  public void setCount(float count) {
//    this.count = count;
//  }
  
//  public float getSum() { return sum; }
//
//  public float getStdev() { return stdev; }
//
//  public float getCount() { return count; }
  
  public abstract boolean confidenceIntervalTest(Number[] sample, final int offset,
                                                 final float error,
                                                 final float confidence,
                                                 final int windowSize,
                                                 List<Number> result);
}
