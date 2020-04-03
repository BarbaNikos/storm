package gr.katsip.spear;

import org.apache.storm.spear.ScalarAggregate;

import java.io.Serializable;
import java.util.List;

/**
 * @author Nikos R. Katsipoulakis (nick.katsip@gmail.com)
 */
public class ApproxQuantileAggregation extends ScalarAggregate implements Serializable {
  
  @Override
  public boolean confidenceIntervalTest(Number[] sample, int offset, float error,
                                        float confidence, int windowSize,
                                        List<Number> result) {
    int intError = (int) (100 * error);
    int intConfidence = (int) (100 * confidence);
    ApproxSampleQuantile quantile = new ApproxSampleQuantile(intError, intConfidence);
    if (sample.length < quantile.getTotalSize()) {
      return false;
    }
    quantile.load(sample, offset);
    int medianValue = quantile.output(0.5f);
    if (medianValue != Integer.MIN_VALUE) {
      result.add((float) medianValue);
    } else {
      result.add(Float.NaN);
    }
    return true;
  }
  
}
