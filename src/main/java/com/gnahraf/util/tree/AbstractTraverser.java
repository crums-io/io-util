/*
 * Copyright 2008-2020 Babak Farhang
 */
package com.gnahraf.util.tree;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import com.gnahraf.util.CollectionUtils;

/**
 * A depth-first, tree structure traverser. The traverser
 * implements an Euler traversal over the tree structure (generalized
 * for tree nodes with arbitrary number of children). As it traverses
 * the tree, an instance fires pre-order and post-order events along the way.
 * <p/>
 * The tree structure does not have to be defined by the parametric
 * type <tt>&lt;T&gt;</tt> itself (although it typically is); rather,
 * it is the implementation (a subclass of this class) that decides
 * the structure. This is done by implementing the following abstract
 * methods:
 * <ul>
 * <li>{@link #hasChildren(Object)} hasChildren(T node)</li>
 * <li>{@link #getChildren(Object) getChildren(T node)}</li>
 * </ul>
 * <small>
 * (Note we could had collapsed all the requirements into the single
 * method <tt>getChildren(T node)</tt>, but then would have had to
 * specify whether return value can be <tt>null</tt>, zero-length, etc.
 * So hopefully this is clearer.)
 * </small>
 * <p/>
 * <h3>Thread safety</h3>
 * <p>
 * <em>This class is not safe under concurrent access.</em> We'd have
 * to make the fields volatile for that to work.</p>
 * 
 * <p><small><i>
 * From the Skwish project at sourceforge.
 * </i></small></p>
 *
 * @see TraverseListener
 * @see Search:
 *      <a href="http://www.google.com/search?q=euler+traversal">Euler
 *      Traversal</a>
 *
 */
public abstract class AbstractTraverser<T> implements Runnable {
    
  private Comparator<T> siblingOrder;
  
  private final List<TraverseFrame<T>> stack
      = new ArrayList<TraverseFrame<T>>();
  
  private TraverseListener<T> listener;
  
  
  /**
   * Creates a new instance with the given <tt>root</tt> node.
   */
  protected AbstractTraverser(T root) {
      Objects.requireNonNull(root, "null root");
      stack.add(new TraverseFrame<T>(Collections.singletonList(root)));
  }
  
  

  
  /**
   * Sets the listener for the traversal. May be <tt>null</tt>.
   */
  public void setListener(TraverseListener<T> listener) {
      this.listener = listener;
  }
  
  /**
   * Returns the listener set for traversal. May be <tt>null</tt>.
   */
  public TraverseListener<T> getListener() {
      return listener;
  }
  
  
  /**
   * Sets the comparator used to determine the order in which sibling
   * nodes are visited.
   */
  public void setSiblingOrder(Comparator<T> siblingOrder) {
      this.siblingOrder = siblingOrder;
  }
  
  
  /**
   * Returns the comparator used to determine the order in which sibling
   * nodes are visited.
   */
  public Comparator<T> getSiblingOrder() {
      return siblingOrder;
  }
  
  /**
   * Traverses the tree and visits the tree nodes in pre-order.
   * This is done by setting
   * the {@linkplain #setListener(TraverseListener) listener} to be a
   * pre-order adapter on the specified <tt>visitor</tt>.
   * 
   * @throws IllegalStateException
   *         if the tree has already been traversed
   */
  public void visitPreorder(final Consumer<T> visitor) {
      TraverseListener<T> adapter = new TraverseListener<T>() {
          public void preorder(T node) {
              visitor.accept(node);
          }
          public void postorder(T node) {
          }
      };
      setListener(adapter);
      run();
  }

  
  /**
   * Traverses the tree and visits the tree nodes in post-order.
   * This is done by setting
   * the {@linkplain #setListener(TraverseListener) listener} to be a
   * post-order adapter on the specified <tt>visitor</tt>.
   * 
   * @throws IllegalStateException
   *         if the tree has already been traversed
   */
  public void visitPostorder(final Consumer<T> visitor) {
      TraverseListener<T> adapter = new TraverseListener<T>() {
          public void preorder(T node) {
          }
          public void postorder(T node) {
              visitor.accept(node);
          }
      };
      setListener(adapter);
      run();
  }
  
  /**
   * Performs the traversal over the tree structure. Pre- and post-order
   * events are fired to the {@linkplain #setListener(TraverseListener)
   * listener}, if any.
   * <p/>
   * <em>This method may only be invoked once. Otherwise, an exception is
   * thrown.</em>
   * 
   * @throws IllegalStateException
   *         if the tree has already been traversed
   */
  public void run() {
    synchronized (stack) {
      if (stack.isEmpty())
        throw new IllegalStateException("already run");
      while (true) {
        TraverseFrame<T> frame = stack.get(stack.size() - 1);
        T current = frame.current();
        firePreorder(current);
        if (hasChildren(current)) {
          T[] descendents = getChildren(current);
          if (descendents.length > 0) {
            Comparator<T> order = siblingOrder;
            if (order != null)
                Arrays.sort(descendents, siblingOrder);
            TraverseFrame<T> nextFrame
                = new TraverseFrame<T>(
                        CollectionUtils.asReadOnlyList(descendents));
            stack.add(nextFrame);
            continue;
          }
        }
        firePostorder(current);
        while (frame.next() == null) {
          stack.remove(stack.size() - 1);
          if (stack.isEmpty())
              return;
          frame = stack.get(stack.size() - 1);
          current = frame.current();
          firePostorder(current);
        }
      }
    }
  }
  
  /**
   * Determines whether the given <tt>node</tt> has child nodes. Note, it's
   * okay if an implementation returns <tt>true</tt> for a given <tt>node</tt>
   * and returns an empty array on invoking {@linkplain #getChildren(Object)
   * getChildren(node)}. I.e. a subclass may treat the semantics of this
   * method as if it were named <em>canHaveChildren</em>.
   */
  protected abstract boolean hasChildren(T node);
  
  /**
   * Returns the child nodes for the specified <tt>node</tt>.
   * This method is invoked only if {@linkplain #hasChildren(Object)
   * hasChildren(node)} returns <tt>true</tt>.
   * 
   * @return an array of child nodes, possibly empty; never <tt>null</tt>.
   */
  protected abstract T[] getChildren(T node);

  
  

  
  
  
  private void firePreorder(T current) {
    TraverseListener<T> client = this.listener;
    if (client != null)
      client.preorder(current);
  }
  
  private void firePostorder(T current) {
    TraverseListener<T> client = this.listener;
    if (client != null)
      client.postorder(current);
  }

}
