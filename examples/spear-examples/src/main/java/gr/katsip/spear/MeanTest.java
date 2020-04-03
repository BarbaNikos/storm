package gr.katsip.spear;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.storm.spear.ScalarAggregate;

import java.io.Serializable;
import java.util.List;

public class MeanTest extends ScalarAggregate implements Serializable {
  
  private final double zp;
  
  public MeanTest(final float confidence) {
    NormalDistribution standardNormal = new NormalDistribution(0.0f, 1.0f);
    zp = standardNormal.inverseCumulativeProbability(confidence);
  }
  
  @Override
  public boolean confidenceIntervalTest(Number[] sample, int offset, float error, float confidence,
                                        int windowSize, List<Number> result) {
    double mean;
    double stdev;
    if (offset >= windowSize || offset <= 0) {
      return true;
    }
    SummaryStatistics stat = new SummaryStatistics();
    for (int i = 1; i < offset; ++i) {
      stat.addValue(sample[i].floatValue());
    }
    mean = stat.getMean();
    stdev = stat.getStandardDeviation();
    float ci = (float) (zp * stdev) / (float) Math.sqrt(sample.length);
    double upperError = Math.abs(mean - (mean + ci)) / mean;
    if (mean == 0.0f)
      upperError = ci;
    boolean accel = upperError < error;
    result.add(mean);
    return accel;
  }
}
