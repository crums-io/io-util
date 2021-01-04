/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util;


import java.util.Objects;


/**
 * Something similar should be in the standard library. Instances are immutable, if their members
 * are immutable.
 */
public class Tuple<A, B> {
      
  
  /**
   * Non-null.
   */
  public final A a;

  /**
   * Non-null.
   */
  public final B b;

  
  /**
   * Constructs a new instance with the given non-null arguments.
   */
  public Tuple(A a, B b) {
    this.a = Objects.requireNonNull(a, "null a");
    this.b = Objects.requireNonNull(b, "null b");
  }
  
  
  /**
   * Copy constructor.
   */
  protected Tuple(Tuple<A,B> copy) {
    Objects.requireNonNull(copy, "null copy");  // so the subclass doesn't have to
    this.a = copy.a;
    this.b = copy.b;
  }
  

  /**
   * Two instances are equal iff their members are equal.
   */
  @Override
  public final boolean equals(Object o) {
    if (this == o)
      return false;
    if (o instanceof Tuple) {
      Tuple<?, ?> tuple = (Tuple<?, ?>) o;
      return tuple.a.equals(a) && tuple.b.equals(b);
    } else
      return false;
  }
  

  /**
   * Consistent with {@linkplain #equals(Object)}.
   */
  @Override
  public final int hashCode() {
    return a.hashCode() + 31 * b.hashCode();
  }
  
  
  /**
   * Returns {@code "(" + a + "," + b + ")"}.
   */
  public String toString() {
    return "(" + a + "," + b + ")";
  }

}
