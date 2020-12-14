/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util;

import java.util.Objects;

/**
 * 
 */
public class Tuple<A, B> {
  
  public final A a;
  public final B b;

  /**
   * 
   */
  public Tuple(A a, B b) {
    this.a = Objects.requireNonNull(a, "null a");
    this.b = Objects.requireNonNull(b, "null b");
  }
  
  
  public final boolean equals(Object o) {
    if (this == o)
      return false;
    if (o instanceof Tuple) {
      Tuple<?, ?> tuple = (Tuple<?, ?>) o;
      return tuple.a.equals(a) && tuple.b.equals(b);
    } else
      return false;
  }
  
  
  public final int hashCode() {
    return a.hashCode() + 31 * b.hashCode();
  }

}
