/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.util;

/**
 * <p>
 * Base class for class-equivalent objects. With class-equivalent objects, instances
 * belonging to the same class are equal.
 * </p><p>
 * Note that derived instances can safely override
 * {@linkplain #equals(Object)} without breaking the required ({@linkplain Object#equals(Object)
 * equality semantics}. (Usually, when a class overrides <code>equals()</code>, its subclasses may not
 * override it again without breaking the contract.) 
 * </p>
 * 
 * @author Babak
 */
public abstract class ClassEquivalent {
  

  /**
   * @return <code>true</code> iff <code>other</code>'s class is the same as this one's.
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
