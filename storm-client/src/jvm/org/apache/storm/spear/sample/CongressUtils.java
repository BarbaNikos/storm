package org.apache.storm.spear.sample;

import com.google.common.base.Preconditions;
import org.apache.storm.spear.window.ReservoirSample;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class CongressUtils<K> {
  
  private final int budget;
  
  private float sumOfNominators;
  
  private int totalSampleSize;
  
  private float second;
  
  private HashMap<K, ReservoirSample> reservoirs;
  
  private CongressUtils(final int budget) {
    this.budget = budget;
  }
  
  public static <K> HashMap<K, ReservoirSample> sample(final int budget, final List<Tuple> tuples,
                                                       final Map<K, Integer> histogram,
                                                       final Function<Object, K> keyExtractor,
                                                       final Function<Object, Number> valueExtractor) {
    CongressUtils utils = new CongressUtils<K>(budget);
    utils.init();
    utils.determineSampleSizePerGroup(tuples.size(), histogram);
    utils.sample(tuples, keyExtractor, valueExtractor);
    return utils.getReservoirs();
  }
  
  private HashMap<K, ReservoirSample> getReservoirs() {
    return reservoirs;
  }
  
  private void init() {
    totalSampleSize = 0;
    sumOfNominators = 0.0f;
    reservoirs = new HashMap<>();
  }
  
  private void determineSampleSizePerGroup(final int numberOfTuples, final Map<K, Integer> histogram) {
    Preconditions.checkArgument(numberOfTuples >= 0,
        "number of tuples cannot be <= 0.");
    float second = 1.0f / (float) histogram.size();
    float sumOfNominators = 0.0f;
    HashMap<K, Float> noms = new HashMap<>();
    for (Map.Entry<K, Integer> e : histogram.entrySet()) {
      int Ng = e.getValue();
      float first = (float) Ng / (float) numberOfTuples;
      float nom = Math.max(first, second);
      noms.put(e.getKey(), nom);
      sumOfNominators += nom;
    }
    for (Map.Entry<K, Float> e : noms.entrySet()) {
      int Cg = (int) (((float) budget) * (e.getValue() / sumOfNominators));
      if (Cg < 1) {
        Cg = 1;
      }
      reservoirs.put(e.getKey(), new ReservoirSample(Cg));
      totalSampleSize += Cg;
    }
  }
  
  private void sample(final List<Tuple> tuples, final Function<Object, K> keyExtractor,
                     final Function<Object, Number> valueExtractor) {
    int missingGroupSize = (int) (((float) budget) * (second / sumOfNominators));
    if (missingGroupSize < 1) {
      missingGroupSize = 2;
    }
    for (Tuple t : tuples) {
      K key = keyExtractor.apply(t);
      Number value = valueExtractor.apply(t);
      if (reservoirs.containsKey(key)) {
        reservoirs.get(key).addValue(value);
      } else {
        reservoirs.put(key, new ReservoirSample(missingGroupSize));
        reservoirs.get(key).addValue(value);
      }
    }
  }
  
}
