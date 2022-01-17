/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.buffer;


import static io.crums.io.buffer.BufferUtils.binaryFirst;
import static io.crums.io.buffer.BufferUtils.binaryLast;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

import io.crums.io.store.Sorted;

/**
 * Marker for a sorted <tt>Block</tt>. There's no real guarantee that a given instance's
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
   *        assumed to contain a sorted array of cells in ascending <tt>order</tt>;
   *        results are undefined otherwise.
   * @param order
   *        if <tt>null</tt>, then in lexical order
   */
  public SortedBlock(Block copy, Comparator<ByteBuffer> order) {
    super(copy);
    this.order = order;
  }

  /**
   * Creates a new instance over the given <tt>block</tt>, assumed to contain cells
   * in sorted <tt>order</tt>.
   * 
   * @param order
   *        if <tt>null</tt>, then searched in lexical order
   * 
   * @see Block#BufferGroup(ByteBuffer, int, boolean)
   */
  public SortedBlock(ByteBuffer block, int cellByteWidth, Comparator<ByteBuffer> order) {
    this(block, cellByteWidth, order, false);
  }

  /**
   * Creates a new instance over the given <tt>block</tt>, assumed to contain cells
   * in sorted <tt>order</tt>.
   * 
   * @param order
   *        if <tt>null</tt>, then searched in lexical order
   * 
   * @see Block#BufferGroup(ByteBuffer, int, boolean)
   */
  public SortedBlock(
      ByteBuffer block, int cellByteWidth, Comparator<ByteBuffer> order, boolean readOnlyCells) {
    
    super(block, cellByteWidth, readOnlyCells);
    this.order = order;
  }
  
  
  /**
   * Returns the (assumed) cell ordering. If <tt>null</tt>, then lexical ordering is assumed.
   */
  public final Comparator<ByteBuffer> order() {
    return order;
  }
  
  
  /**
   * Tests whether the cells are actually sorted.
   * 
   * @return <tt>true</tt> iff the cells in this instance are sorted in ascending order
   * 
   */
  public boolean isSorted() {
    return BufferUtils.isSorted(cells, order);
  }
  
  
  
  /**
   * Binary searches for the specified <tt>key</tt> and returns its index.
   * <pre><tt>
      public int binarySearch(ByteBuffer key) {
        return Arrays.binarySearch(cells, key, order);
      }
   * </tt></pre>
   * 
   * @see BufferUtils#binarySearch(ByteBuffer[], ByteBuffer, Comparator)
   */
  public int binarySearch(ByteBuffer key) {
    return Arrays.binarySearch(cells, key, order);
  }
  
  
  /**
   * Binary searches for the specified key and and returns its index.
   * <pre><tt>
      public int binarySearch(ByteBuffer key, int fromIndex, int toIndex) {
        return Arrays.binarySearch(cells, fromIndex, toIndex, key, order);
      }
   * </tt></pre>
   * 
   * @param fromIndex
   *        the first cell index searched (inclusive)
   * @param toIndex
   *        the last cell index searched (exclusive)
   * @return
   *        the cell index if found; -<em>insertionPoint</em> - 1, if the
   *        <tt>key</tt> is not found
   */
  public int binarySearch(ByteBuffer key, int fromIndex, int toIndex) {
    return Arrays.binarySearch(cells, fromIndex, toIndex, key, order);
  }
  
  
  /**
   * Returns the first index of the specified <tt>key</tt>.
   * 
   * @see BufferUtils#binaryFirst(ByteBuffer[], ByteBuffer, Comparator)
   */
  public int indexOf(ByteBuffer key) {
    return binaryFirst(cells, key, order);
  }
  
  
  /**
   * Returns the last index of the specified <tt>key</tt>.
   * 
   * @see BufferUtils#binaryLast(ByteBuffer[], ByteBuffer, Comparator)
   */
  public int lastIndexOf(ByteBuffer key) {
    return binaryLast(cells, key, order);
  }
  
  
  /**
   * Compares the given <tt>key</tt> to the cell at the specified <tt>index</tt>.
   */
  public int compareToCell(ByteBuffer key, int index) {
    return order.compare(key, cells[index]);
  }

}
