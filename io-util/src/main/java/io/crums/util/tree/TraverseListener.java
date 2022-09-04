/*
 * Copyright 2008-2020 Babak Farhang
 */
package io.crums.util.tree;


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
   * Invoked by the <code>Traverser</code> on traversing a <code>node</code>
   * in pre-order. Every <code>node</code> in the tree structure is
   * guaranteed to be represented by exactly one invocation of this
   * method.
   */
  void preorder(T node);
  
  /**
   * Invoked by the <code>Traverser</code> on traversing a <code>node</code>
   * in post-order. Every <code>node</code> in the tree structure is
   * guaranteed to be represented by exactly one invocation of this
   * method.
   */
  void postorder(T node);

}
