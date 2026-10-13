/*
 * Copyright 2008-2020 Babak Farhang
 */
package io.crums.util.tree;


import java.util.List;
import java.util.Objects;

/**
 * The representation of tree nodes at a particular depth.
 * The <tt>AbstractTraverser</tt> class uses a stack of these internally to represent
 * state during a traversal.
 * <p><small><i>
 * From the Skwish project at sourceforge.
 * </i></small></p>
 */
class TraverseFrame<T> {
    
  private final List<T> siblings;
  private int index;
  
  public TraverseFrame(List<T> siblings) {
    this.siblings = Objects.requireNonNull(siblings, "null siblings");
    if (siblings.isEmpty())
      throw new IllegalArgumentException("empty siblings");
  }
  
  
  
  public T current() {
    return siblings.get(index);
  }
  
  public T next() {
    return index == siblings.size() - 1 ? null : siblings.get(++index);
  }

}