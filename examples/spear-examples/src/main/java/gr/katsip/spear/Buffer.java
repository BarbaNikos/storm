package gr.katsip.spear;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Nikos R. Katsipoulakis (nick.katsip@gmail.com)
 */
class Buffer {
  
  enum State {
    EMPTY,
    FULL
  }
  
  private State state;
  
  final int k;
  
  List<Integer> data;
  
  private int weight;
  
  private int level;
  
  Buffer(final int k) {
    this.k = k;
    this.data = new ArrayList<>(k);
    init();
  }
  
  public int newOperation(int[] data) {
    init();
    this.weight = 1;
    this.state = State.FULL;
    for (int d : data) {
      ingest(d);
    }
    return augment();
  }
  
  public int augment() {
    while (this.data.size() < k) {
      this.data.add(Integer.MIN_VALUE);
      this.data.add(Integer.MAX_VALUE);
    }
    Collections.sort(this.data);
    return data.size();
  }
  
  public boolean ingest(int element) {
    this.data.add(element);
    if (this.data.size() >= k) {
      return true;
    }
    return false;
  }
  
  public void setState(State state) { this.state = state; }
  
  public State getState() { return this.state; }
  
  public void init() {
    this.data.clear();
    this.state = State.EMPTY;
    this.weight = 0;
    this.level = -1;
  }
  
  public int getLevel() { return this.level; }
  
  public void setLevel(int level) { this.level = level; }
  
  public int getWeight() { return weight; }
  
  public void setWeight(int weight) { this.weight = weight; }
  
}
