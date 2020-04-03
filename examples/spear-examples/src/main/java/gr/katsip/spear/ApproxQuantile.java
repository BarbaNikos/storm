package gr.katsip.spear;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Nikos R. Katsipoulakis (nick.katsip@gmail.com)
 */
public class ApproxQuantile {
  
  private final int b;
  
  private final int k;
  
  private Buffer[] buffers;
  
  private int count;
  
  private int streamLength;
  
  /**
   * number of Collapse operations (i.e., number of non-leaf non-root nodes
   */
  private int C;
  
  /**
   * Sum of weights of all COLLAPSE operations
   */
  private int W;
  
  /**
   * Weight of the heaviest COLLAPSE
   */
  private int wMax;
  
  public ApproxQuantile(final float epsilon, final long N) {
    int b = ApproxConfiguration.getBValue(epsilon, N);
    int k = ApproxConfiguration.getKValue(epsilon, N);
    this.b = b;
    this.k = k;
    this.buffers = new Buffer[this.b];
    for (int i = 0; i < buffers.length; ++i)
      this.buffers[i] = new Buffer(this.k);
    this.count = 0;
    this.streamLength = 0;
    this.wMax = 0;
  }
  
  public ApproxQuantile(final int b, final int k) {
    this.b = b;
    this.k = k;
    this.buffers = new Buffer[this.b];
    for (int i = 0; i < buffers.length; ++i)
      this.buffers[i] = new Buffer(this.k);
    this.count = 0;
    this.streamLength = 0;
    this.wMax = 0;
  }
  
  public void load(Number[] data, int offset) {
    ArrayList<Integer> buffer = new ArrayList<>(k);
    for (int i = 0; i < offset; ++i) {
      buffer.add(data[i].intValue());
      if (buffer.size() >= k) {
        int[] bufferArray = new int[buffer.size()];
        for (int j = 0; j < buffer.size(); ++j)
          bufferArray[j] = buffer.get(j);
        segmentLoad(bufferArray);
        buffer.clear();
      }
    }
    if (buffer.size() > 0) {
      int[] bufferArray = new int[buffer.size()];
      for (int j = 0; j < buffer.size(); ++j)
        bufferArray[j] = buffer.get(j);
      segmentLoad(bufferArray);
    }
  }
  
  /**
   * This receives chunks of up to k elements
   * @param data an array of up to k elements
   */
  public void segmentLoad(int[] data) {
    int emptyIndex = -1;
    int emptyCount = 0;
    int minLevel = Integer.MAX_VALUE;
    for (int i = 0; i < buffers.length; ++i) {
      if (buffers[i].getLevel() >= 0 && minLevel > buffers[i].getLevel())
        minLevel = buffers[i].getLevel();
      if (buffers[i].getState() == Buffer.State.EMPTY) {
        emptyIndex = i;
        emptyCount++;
      }
    }
    if (emptyCount >= 2) {
      /**
       * invoke new on the empty buffer and assign its level as 0
       */
      buffers[emptyIndex].init();
      int actualElementsAdded = buffers[emptyIndex].newOperation(data);
      buffers[emptyIndex].setLevel(0);
      count += actualElementsAdded;
      streamLength += data.length;
    } else if (emptyCount == 1) {
      /**
       * invoke new on the empty buffer and assign its level as minLevel
       */
      buffers[emptyIndex].init();
      int elementsAdded = buffers[emptyIndex].newOperation(data);
      buffers[emptyIndex].setLevel(minLevel);
      count += elementsAdded;
      streamLength += data.length;
    } else {
      /**
       * Collapse all the buffers with level = minLevel and assign the output buffer level
       * minLevel + 1
       */
      List<Integer> bufferIndices = new ArrayList<>();
      for (int i = 0; i < buffers.length; ++i) {
        if (buffers[i].getLevel() == minLevel)
          bufferIndices.add(i);
      }
      collapse(bufferIndices, minLevel);
      segmentLoad(data);
    }
  }
  
  public int getNumberOfLeaves() {
    int L = 0;
    for (Buffer b : buffers) {
      if (b.getWeight() == 1)
        L++;
    }
    return L;
  }
  
  public int getHeight() {
    int maxLevel = Integer.MIN_VALUE;
    for (Buffer b : buffers) {
      if (maxLevel < b.getLevel())
        maxLevel = b.getLevel();
    }
    return maxLevel + 1;
  }
  
  public void collapse(List<Integer> bufferIndices, int minLevel) {
    List<Integer> tmp = new ArrayList<>();
    Buffer outputBuffer = new Buffer(k);
    int outputBufferWeight = 0;
    for (int index : bufferIndices) {
      outputBufferWeight += this.buffers[index].getWeight();
      for (Integer d : this.buffers[index].data) {
        for (int i = 0; i < this.buffers[index].getWeight(); ++i)
          tmp.add(d);
      }
      this.buffers[index].init();
    }
    Collections.sort(tmp);
    int outputIndex = bufferIndices.get(0);
    this.buffers[outputIndex].init();
    this.buffers[outputIndex].setLevel(minLevel + 1);
    this.buffers[outputIndex].setState(Buffer.State.FULL);
    this.buffers[outputIndex].setWeight(outputBufferWeight);
    if (outputBufferWeight % 2 != 0) {
      int constant = (outputBufferWeight + 1) / 2;
      for (int j = 0; j < k; ++j)
        this.buffers[outputIndex].data.add(tmp.get(j*outputBufferWeight + constant));
    } else {
      int tik = outputBufferWeight / 2;
      int tok = (outputBufferWeight + 2) / 2;
      for (int j = 0; j < k; ++j) {
        if (j % 2 != 0)
          this.buffers[outputIndex].data.add(tmp.get(j*outputBufferWeight + tik));
        else {
          if (j * outputBufferWeight + tok >= tmp.size())
            this.buffers[outputIndex].data.add(tmp.get(j * outputBufferWeight + tok - 1));
          else
            this.buffers[outputIndex].data.add(tmp.get(j * outputBufferWeight + tok));
        }
      }
    }
    C += 1;
    W += outputBufferWeight;
    wMax = wMax < outputBufferWeight ? outputBufferWeight : wMax;
  }
  
  public int output(final float phi) {
    if (phi < 0.0f || phi > 1.f)
      throw new IllegalArgumentException("invalid value for phi");
    final float beta = ((float) count) / ((float) streamLength);
    final float phiPrime = (2.0f * phi + beta - 1.f) / (2.0f * beta);
    int totalWeight = 0;
    List<Integer> tmp = new ArrayList<>();
    for (int i = 0; i < buffers.length; ++i) {
      if (buffers[i].getState() == Buffer.State.FULL) {
        totalWeight += buffers[i].getWeight();
        for (Integer d : this.buffers[i].data) {
          for (int j = 0; j < this.buffers[i].getWeight(); ++j)
            tmp.add(d);
        }
      }
    }
    Collections.sort(tmp);
    int finalIndex = (int) Math.ceil(phiPrime * this.k * totalWeight);
    if (tmp.size() >= finalIndex) return tmp.get(finalIndex);
    return Integer.MIN_VALUE;
  }
  
  public int getTotalSize() { return this.b * this.k; }
  
}
