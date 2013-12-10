/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.util;

import java.util.Comparator;

/**
 * Utility for tracking the minimum and maximum of a corpus.
 * 
 * @author Babak
 */
public abstract class MinMaxObserver<T> {
  
  protected T min;
  protected T max;
  
  
  public void observe(T value) {
    if (value == null)
      return;
    if (min == null) {
      min = max = value;
      return;
    }
    int maxComp = compare(value, max);
    if (maxComp < 0) {
      if (compare(value, min) < 0)
        min = value;
    } else if (maxComp > 0) {
      max = value;
    }
  }
  
  
  protected abstract int compare(T a, T b);
  
  
  /**
   * Returns the minimum observed instance. If multiple instances
   * evaluated equal to the minimum, then the returned instance is
   * the first observed minimum.
   */
  public T min() {
    return min;
  }
  
  /**
   * Returns the maximum observed instance. If multiple instances
   * evaluated equal to the maximum, then the returned instance is
   * the first observed maximum.
   */
  public T max() {
    return max;
  }
  
  /**
   * Clears the instance. {@linkplain #isSet()} returns <tt>false</tt>.
   */
  public void clear() {
    min = max = null;
  }
  
  /**
   * Tells whether any values have been observed.
   */
  public boolean isSet() {
    return min != null;
  }
  
  
  
  public static <T> MinMaxObserver<T> newInstance(Comparator<T> comparator) {
    return new ComparatorImpl<>(comparator);
  }
  
  
  public static <T extends Comparable<T>> MinMaxObserver<T> newInstance() {
    return new ComparableImpl<>();
  }
  
  
  
  
  
  
  protected static class ComparatorImpl<T> extends MinMaxObserver<T> {
    private final Comparator<T> comparator;
    protected ComparatorImpl(Comparator<T> comparator) {
      this.comparator = comparator;
      if (comparator == null)
        throw new IllegalArgumentException("null comparator");
    }
    @Override
    protected int compare(T a, T b) {
      return comparator.compare(a, b);
    }
  }
  
  
  protected static class ComparableImpl<T extends Comparable<T>> extends MinMaxObserver<T> {
    @Override
    protected int compare(T a, T b) {
      return a.compareTo(b);
    }
  }

}
