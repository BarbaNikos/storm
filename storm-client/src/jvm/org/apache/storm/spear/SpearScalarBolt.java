package org.apache.storm.spear;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.windowing.TupleWindow;

public abstract class SpearScalarBolt extends BaseWindowedBolt {
  
  public abstract Number produceResult(TupleWindow inputWindow);
  
  public abstract void handleResult(boolean acceleration, TupleWindow window, Number result);
  
  @Override
  public void execute(TupleWindow inputWindow) {
    Number result = produceResult(inputWindow);
    handleResult(false, inputWindow, result);
  }
}
