package org.apache.storm.spear;

import org.apache.storm.spear.window.ReservoirSample;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WindowStatistics<K> {
  
  private Map<K, Integer> histogram;
  
  private Map<K, ReservoirSample> samples;
  
  private int tupleCount;
  
  private final int budgetPerKey;
  
  private final int budget;
  
  public WindowStatistics(final int budget) {
    this.budget = budget;
    this.histogram = new HashMap<>(budget);
    this.tupleCount = 0;
    budgetPerKey = -1;
  }
  
  public WindowStatistics(final int budget, final int budgetPerKey) {
    this.budget = budget;
    this.samples = new HashMap<>(budget);
    this.tupleCount = 0;
    this.budgetPerKey = budgetPerKey;
  }
  
  public void add(K key) {
    if (!overflow()) {
      histogram.compute(key, (k, v) -> v == null ? 1 : v + 1);
      ++tupleCount;
    }
  }
  
  public void add(K key, Number value) {
    if (samples.containsKey(key) && samples.size() < budget) {
      samples.get(key).addValue(value);
    } else {
      ReservoirSample sample = new ReservoirSample(budgetPerKey);
      sample.addValue(value);
      samples.put(key, sample);
    }
    ++tupleCount;
  }
  
  public boolean overflow() {
    if (budgetPerKey < 0) {
      return histogram.size() >= budget;
    }
    return samples.size() >= budget;
  }
  
  public int getNumberOfKeys() {
    return histogram.size();
  }
  
  public int getNumberOfTuples() {
    return tupleCount;
  }
  
  public int getKeyFrequency(K key) {
    return histogram.getOrDefault(key, 0);
  }
  
  public ReservoirSample getSample(K key) {
    return samples.get(key);
  }
  
  public Set<K> keys() {
    return histogram.keySet();
  }
  
  public Map<K, Integer> getHistogram() {
    return histogram;
  }
  
  public Map<K, ReservoirSample> getSamples() {
    return samples;
  }
}
