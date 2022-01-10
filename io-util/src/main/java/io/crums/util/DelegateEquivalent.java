/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util;

/**
 * Another attempt at minimizing boilerplate code for implementing
 * {@linkplain Object#equals(Object)} and {@linkplain Object#hashCode()}
 * when the logic is driven by a single member in the class.
 * 
 * @see #delegateObj()
 * @see #isSameType(DelegateEquivalent)
 */
public abstract class DelegateEquivalent {
  

  @Override
  public final boolean equals(Object other) {
    if (other == this)
      return true;
    if (other instanceof DelegateEquivalent) {
      var od = (DelegateEquivalent) other;
      Object delegate = delegateObj();
      Object otherDelegate = od.delegateObj();
      return delegate.equals(otherDelegate) && isSameType(od);
    }
    return false;
  }
  
  
  @Override
  public final int hashCode() {
    return delegateObj().hashCode();
  }
  
  
  /**
   * Returns the delegate object for equality and hash code. Implementations
   * should be marked as <em><b>final</b></em>.
   * 
   * @see #isSameType(DelegateEquivalent)
   */
  protected abstract Object delegateObj();
  
  
  /**
   * For best performance override with a class-specific {@code instanceof} test.
   * The base class returns {@code getClass() == other.getClass()}.
   */
  protected boolean isSameType(DelegateEquivalent other) {
    return getClass() == other.getClass();
  }

}
