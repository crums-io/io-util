/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.block;


import static io.crums.io.buffer.BufferUtils.binaryFirst;
import static io.crums.io.buffer.BufferUtils.binaryLast;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

import io.crums.io.buffer.Block;
import io.crums.io.buffer.BufferUtils;
import io.crums.io.store.Sorted;

/**
 * Marker for a sorted <code>Block</code>. There's no real guarantee that a given instance's
 * underlying buffer's contents is sorted; that would be too expensive to enforce, and
 * consequently sorted-ness is honorary. Suffice to say, results are defined when
 * you break the rules.
 * 
 * @author Babak
 */
public class SortedBlock extends Block implements Sorted {
  
  private final Comparator<ByteBuffer> order;
  
  /**
   * Creates an instance from an ordinary one.
   * 
   * @param copy
   *        assumed to contain a sorted array of cells in ascending <code>order</code>;
   *        results are undefined otherwise.
   * @param order
   *        if <code>null</code>, then in lexical order
   */
  public SortedBlock(Block copy, Comparator<ByteBuffer> order) {
    super(copy);
    this.order = order;
  }

  /**
   * Creates a new instance over the given <code>block</code>, assumed to contain cells
   * in sorted <code>order</code>.
   * 
   * @param order
   *        if <code>null</code>, then searched in lexical order
   */
  public SortedBlock(ByteBuffer block, int cellByteWidth, Comparator<ByteBuffer> order) {
    this(block, cellByteWidth, order, false);
  }

  /**
   * Creates a new instance over the given <code>block</code>, assumed to contain cells
   * in sorted <code>order</code>.
   * 
   * @param order
   *        if <code>null</code>, then searched in lexical order
   */
  public SortedBlock(
      ByteBuffer block, int cellByteWidth, Comparator<ByteBuffer> order, boolean readOnlyCells) {
    
    super(block, cellByteWidth, readOnlyCells);
    this.order = order;
  }
  
  
  /**
   * Returns the (assumed) cell ordering. If <code>null</code>, then lexical ordering is assumed.
   */
  public final Comparator<ByteBuffer> order() {
    return order;
  }
  
  
  /**
   * Tests whether the cells are actually sorted.
   * 
   * @return <code>true</code> iff the cells in this instance are sorted in ascending order
   * 
   */
  public boolean isSorted() {
    return BufferUtils.isSorted(cells, order);
  }
  
  
  
  /**
   * Binary searches for the specified <code>key</code> and returns its index.
   * <pre><code>
      public int binarySearch(ByteBuffer key) {
        return Arrays.binarySearch(cells, key, order);
      }
   * </code></pre>
   * 
   * @see BufferUtils#binarySearch(ByteBuffer[], ByteBuffer, Comparator)
   */
  public int binarySearch(ByteBuffer key) {
    return Arrays.binarySearch(cells, key, order);
  }
  
  
  /**
   * Binary searches for the specified key and and returns its index.
   * <pre><code>
      public int binarySearch(ByteBuffer key, int fromIndex, int toIndex) {
        return Arrays.binarySearch(cells, fromIndex, toIndex, key, order);
      }
   * </code></pre>
   * 
   * @param fromIndex
   *        the first cell index searched (inclusive)
   * @param toIndex
   *        the last cell index searched (exclusive)
   * @return
   *        the cell index if found; -<em>insertionPoint</em> - 1, if the
   *        <code>key</code> is not found
   */
  public int binarySearch(ByteBuffer key, int fromIndex, int toIndex) {
    return Arrays.binarySearch(cells, fromIndex, toIndex, key, order);
  }
  
  
  /**
   * Returns the first index of the specified <code>key</code>.
   * 
   * @see BufferUtils#binaryFirst(ByteBuffer[], ByteBuffer, Comparator)
   */
  public int indexOf(ByteBuffer key) {
    return binaryFirst(cells, key, order);
  }
  
  
  /**
   * Returns the last index of the specified <code>key</code>.
   * 
   * @see BufferUtils#binaryLast(ByteBuffer[], ByteBuffer, Comparator)
   */
  public int lastIndexOf(ByteBuffer key) {
    return binaryLast(cells, key, order);
  }
  
  
  /**
   * Compares the given <code>key</code> to the cell at the specified <code>index</code>.
   */
  public int compareToCell(ByteBuffer key, int index) {
    return order.compare(key, cells[index]);
  }

}
