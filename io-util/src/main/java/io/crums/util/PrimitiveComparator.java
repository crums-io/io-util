/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.util;

import java.util.Comparator;
import java.util.Objects;

/**
 * Supports comparing mixed primitive types.
 */
public final class PrimitiveComparator implements Comparator<Number> {
  
  private final static double MAX_LONG = Long.MAX_VALUE;
  private final static double MIN_LONG = Long.MIN_VALUE;
  
  /**
   * Global instance.
   */
  public final static PrimitiveComparator INSTANCE = new PrimitiveComparator();

  private PrimitiveComparator() { }
  
  @Override
  public int compare(Number a, Number b) {
    return compareNumbers(a, b);
  }
  
  public static int compareNumbers(Number a, Number b) {
    if (a == b)
      return 0;
    double aD = a.doubleValue();
    double bD = b.doubleValue();
    int comp = Double.compare(aD, bD);
    if (comp == 0 && inLongBounds(aD))
      comp = Long.compare(a.longValue(), b.longValue());
    return comp;
  }
  
  
  private static boolean inLongBounds(double value) {
    return
        Double.compare(value, MAX_LONG) <= 0 &&
        Double.compare(value, MIN_LONG) >= 0;
  }
  
  
  public static boolean equal(Number a, Number b) {
    if (a == null)
      return b == null;
    return b != null && compareNumbers(a, b) == 0;
  }
  
  
  public static int hashCode(Number n) {
    Objects.requireNonNull(n, "null number argument");
    return Long.hashCode(n.longValue()) * 31 + Double.hashCode(n.doubleValue());
  }

}
