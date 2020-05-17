/*
 * Copyright 2008-2020 Babak Farhang
 */
package com.gnahraf.util.tree;


/**
 * A listener for depth-first traversal of a tree structure.
 * 
 * <p><small><i>
 * From the Skwish project at sourceforge.
 * </i></small></p>
 * 
 * @see AbstractTraverser#setListener(TraverseListener)
 */
public interface TraverseListener<T> {
    
  /**
   * Invoked by the <tt>Traverser</tt> on traversing a <tt>node</tt>
   * in pre-order. Every <tt>node</tt> in the tree structure is
   * guaranteed to be represented by exactly one invocation of this
   * method.
   */
  void preorder(T node);
  
  /**
   * Invoked by the <tt>Traverser</tt> on traversing a <tt>node</tt>
   * in post-order. Every <tt>node</tt> in the tree structure is
   * guaranteed to be represented by exactly one invocation of this
   * method.
   */
  void postorder(T node);

}
