/*
 * Copyright 2014 Babak Farhang 
 */
package io.crums.math.stats;


/**
 * Keeps track of a moving average over a fixed size window. This class is
 * suited for tracking discrete, integral values. Note the computational
 * complexity of computing the average is independent of the
 * {@linkplain #windowSize() window size}.
 * 
 * @author Babak
 */
public class MovingAverage {
  
  
  private final long[] window;
  /**
   * Sum of the values in <code>window</code>. Always up-to-date.
   */
  private long wSum;
  /**
   * Circular index into <code>window</code>. Range invariant: [0, window.length).
   */
  private int wIndex;
  private long count;
  

  /**
   * Creates a new instance initialized to zero with the given <code>windowSize</code>.
   */
  public MovingAverage(int windowSize) {
    this(windowSize, 0);
  }


  /**
   * Creates a new instance with the given <code>windowSize</code>, initialized to the
   * given <code>initValue</code>.
   */
  public MovingAverage(int windowSize, long initValue) {
    if (windowSize < 1)
      throw new IllegalArgumentException("windowSize: " + windowSize);
    
    this.window = new long[windowSize];
    
    if (initValue != 0)
      clear(initValue);
  }
  
  
  
  public final long valueAt(int windowIndex) throws IndexOutOfBoundsException {
    return window[(windowIndex + wIndex) % window.length];
  }
  
  
  public final long lastValue() {
    return valueAt(window.length - 1);
  }
  

  /**
   * Clears the instance, and initializes the average to zero.
   */
  public final void clear() {
    clear(0);
  }
  
  
  /**
   * Clears the instance, and initializes the average to the given
   * <code>initValue</code>.
   */
  public void clear(long initValue) {
    for (int index = window.length; index-- > 0; )
      window[index] = initValue;
    wSum = window.length * initValue;
    count = wIndex = 0;
  }
  
  
  /**
   * Returns the number of elements in the moving average.
   */
  public final int windowSize() {
    return window.length;
  }
  
  
  /**
   * Returns the moving average. O(1) operation.
   */
  public final long average() {
    return wSum / window.length;
  }
  
  /**
   * Returns the moving average in double precision. O(1) operation.
   */
  public final double precisionAverage() {
    return ((double) wSum) / window.length;
  }
  
  
  /**
   * Returns the number of the values observed since the instance was last
   * {@linkplain #clear() clear}ed.
   */
  public final long count() {
    return count;
  }
  
  
  public void observe(long value) {
    wSum += value - window[wIndex];
    window[wIndex] = value;
    ++count;
    wIndex = (wIndex + 1) % window.length;
  }

}
