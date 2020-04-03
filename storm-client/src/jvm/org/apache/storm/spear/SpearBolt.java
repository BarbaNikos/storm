package org.apache.storm.spear;

import org.apache.storm.spear.window.ReservoirSample;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.windowing.TupleWindow;

import java.util.Map;

public abstract class SpearBolt<K> extends BaseWindowedBolt {
  
  public enum ErrorType {
    MEAN_ERROR,
    MAX_ERROR
  }
  
  private ErrorType errorType = ErrorType.MEAN_ERROR;
  
  protected SpearBolt(ErrorType errorType) {
    this.errorType = errorType;
  }
  
  public abstract Map<K, Number> produceResult(TupleWindow inputWindow);
  
  public abstract void handleResult(boolean acceleration, TupleWindow window,
                                    Map<K, Number> result);
  
  @Override
  public void execute(TupleWindow inputWindow) {
    Map<K, Number> result = produceResult(inputWindow);
    handleResult(false, inputWindow, result);
  }
  
  public final Number estimateError(Map<K, ReservoirSample> sample, Map<K, Number> result) {
    double aggregateError = 0.0;
    for (Map.Entry<K, ReservoirSample> e : sample.entrySet()) {
      double groupError = estimateGroupError(result, e.getKey(), e.getValue());
      switch (errorType) {
        case MEAN_ERROR:
          aggregateError += groupError;
          break;
        case MAX_ERROR:
          aggregateError = groupError > aggregateError ? groupError : aggregateError;
          break;
      }
    }
    if (errorType == ErrorType.MEAN_ERROR) {
      return aggregateError / ((double) sample.size());
    }
    return aggregateError;
  }
  
  public abstract double estimateGroupError(Map<K, Number> result, final K group,
                                            final ReservoirSample sample);
}
