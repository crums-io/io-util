/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util;


/**
 * A {@linkplain Tuple tuple} with natural order on the first member {@linkplain Tuple#a}.
 * (I tried to create a generic Comparator instead, but I couldn't get the syntax right,
 * even tho I tried to play the same standard tricks the library does. So I'm down to this.)
 * 
 * @param A a comparable type
 */
public class NaturalTuple<A extends Comparable<A>, B> extends Tuple<A,B> implements Comparable<Tuple<A,B>> {

  
  public NaturalTuple(A a, B b) {
    super(a, b);
  }
  
  /**
   * Promotion constructor. Also doubles as a copy constructor.
   */
  public NaturalTuple(Tuple<A,B> promoted) {
    super(promoted);
  }

  
  /**
   * Compares the instance's {@linkplain Tuple#a} member with that of the other.
   */
  @Override
  public int compareTo(Tuple<A, B> other) {
    return a.compareTo(other.a);
  }

}
