/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.util;

/**
 * Base class for class-equivalent objects. With class-equivalent objects, instances
 * belonging to the same class are equal.
 * 
 * @author Babak
 */
public abstract class ClassEquivalent {
  

  /**
   * @return <tt>true</tt> iff <tt>other</tt>'s class is the same as this one's.
   */
  @Override
  public boolean equals(Object other) {
    return other == this || other != null && other.getClass() == getClass();
  }
  
  /**
   * @return the class hash code
   */
  @Override
  public int hashCode() {
    return getClass().hashCode();
  }

}
