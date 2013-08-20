/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.buffer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Utilities for manipulating buffers. Nothing magical here; just a bit of rigor.
 * 
 * @see BufferOp
 * @author Babak
 */
public class BufferUtils {
  
  private BufferUtils() {  }
  
  /**
   * Positions the given <tt>buffer</tt> at the start of the specified cell. Arguments
   * are not checked.
   * <pre><tt>
   *   int position = cellIndex * cellbytes;
   *   int limit = position + cellbytes;
   *   buffer.limit(limit).position(position);
   * </tt></pre>
   * 
   * 
   * @param cellIndex
   *        zero based cell index (starts at the beginning of the <tt>buffer</tt>)
   * @param cellbytes
   *        number of bytes in each buffer
   * @return
   *        <tt>buffer</tt>
   */
  public static ByteBuffer setCell(ByteBuffer buffer, int cellIndex, int cellbytes) {
    int position = cellIndex * cellbytes;
    int limit = position + cellbytes;
    buffer.limit(limit).position(position);
    return buffer;
  }
  
  
  
  

  
  
  /**
   * Returns the first index of the specified <tt>key</tt>, assuming the <tt>cells</tt> are sorted in
   * ascending <tt>order</tt>. Results are undefined otherwise. If no matching cell is found,
   * then the [negative] index returned has the same semantics as that specified in
   * {@linkplain Arrays#binarySearch(Object[], Object, Comparator)}.
   * 
   * @param order
   *        if <tt>null</tt>, then searched in lexical order
   */
  public static int binaryFirst(ByteBuffer[] cells, ByteBuffer key, Comparator<ByteBuffer> order) {
    int result = Arrays.binarySearch(cells, key, order);
    // make sure its the first one
    while (result > 0) {
      int comp = order.compare(key, cells[result - 1]);
      if (comp > 0)
        break;
      if (comp < 0)
        throw new IllegalStateException("cells are not sorted");
      --result;
    }
    return result;
  }
  

  
  
  /**
   * Returns the last index of the specified <tt>key</tt>, assuming the <tt>cells</tt> are sorted in
   * ascending <tt>order</tt>. Results are undefined otherwise. If no matching cell is found,
   * then the [negative] index returned has the same semantics as that specified in
   * {@linkplain Arrays#binarySearch(Object[], Object, Comparator)}.
   * 
   * @param order
   *        if <tt>null</tt>, then searched in lexical order
   * 
   * @throws NullPointerException
   *         if <tt>key</tt>, <tt>cells</tt>, or any of its elements is <tt>null</tt>
   */
  public static int binaryLast(ByteBuffer[] cells, ByteBuffer key, Comparator<ByteBuffer> order) {
    int result = Arrays.binarySearch(cells, key, order);
    // make sure its the first one
    if (result < 0)
      return result;
    int stop = cells.length - 1;
    while (result < stop) {
      int comp = order.compare(key, cells[result + 1]);
      if (comp < 0)
        break;
      if (comp > 0)
        throw new IllegalStateException("cells are not sorted");
      ++result;
    }
    return result;
  }
  

  
  /**
   * Binary searches for the specified <tt>key</tt>, assuming the <tt>cells</tt> are sorted in
   * ascending <tt>order</tt>. Results are undefined otherwise. If no matching cell is found,
   * then the [negative] index returned has the same semantics as that specified in
   * {@linkplain Arrays#binarySearch(Object[], Object, Comparator)}.
   * <p/>
   * This method behaves exactly as {@linkplain #binaryFirst(ByteBuffer, Comparator) binaryFirst} and
   * {@linkplain #binaryLast(ByteBuffer, Comparator) binaryLast}, if there are no duplicate
   * cells (except that its faster); if there are dups, and there is a hit on a dupe,
   * then there is no guarantee which of the dups is chosen.
   * <pre><tt>
      public int binarySearch(ByteBuffer[] cells, ByteBuffer key, Comparator<ByteBuffer> order) {
        return Arrays.binarySearch(cells, key, order);
      }
   * </tt></pre>
   * 
   * @param order
   *        if <tt>null</tt>, then searched in lexical order
   * 
   * @throws NullPointerException
   *         if <tt>key</tt>, <tt>cells</tt>, or any of its elements is <tt>null</tt>
   */
  public static int binarySearch(ByteBuffer[] cells, ByteBuffer key, Comparator<ByteBuffer> order) {
    return Arrays.binarySearch(cells, key, order);
  }
  
  

  
  
  /**
   * Tests whether the cells are currently sorted.
   * 
   * @param order
   *        if <tt>null</tt>, then searched in lexical order
   * 
   * @throws NullPointerException
   *         if <tt>cells</tt> or any of its elements is <tt>null</tt>
   */
  public static boolean isSorted(ByteBuffer[] cells, Comparator<ByteBuffer> order) {
    int count = cells.length;
    ByteBuffer cell = cells[0];
    for (int i = 1; i < count; ++i) {
      ByteBuffer next = cells[i];
      if (order.compare(cell, next) > 0)
        return false;
      cell = next;
    }
    return true;
  }
  
  
  public static ByteBuffer[] duplicate(ByteBuffer[] buffers) {
    return BufferOp.DUPLICATE.opAll(buffers);
  }
  
  /**
   * Convenience method to clear array of <tt>buffers</tt>.
   * 
   * @throws NullPointerException
   *         if <tt>buffer</tt> or any of its elements is <tt>null</tt>
   */
  public static void clearAll(ByteBuffer[] buffers) {
    for (int i = buffers.length; i-- > 0; )
      buffers[i].clear();
  }
  
  public static ByteBuffer[] slice(ByteBuffer[] buffers) {
    return BufferOp.SLICE.opAll(buffers);
  }
  
  public static ByteBuffer[] asReadOnly(ByteBuffer[] buffers) {
    return BufferOp.AS_READONLY.opAll(buffers);
  }
  
  public static void ensureReadOnly(ByteBuffer[] buffers) {
    BufferOp.ENSURE_READONLY.opAll(buffers);
  }

}
