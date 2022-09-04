/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.buffer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Utilities for manipulating buffers. Nothing magical here; just a bit of rigor.
 * 
 * @see BufferOp
 */
public class BufferUtils {
  
  private BufferUtils() {  }
  
  /**
   * The null buffer. Tho it doesn't have to, it advertises itself as
   * a read-only buffer. Stateless.
   */
  public final static ByteBuffer NULL_BUFFER = ByteBuffer.wrap(new byte[0]).asReadOnlyBuffer();
  
  /**
   * Positions the given <code>buffer</code> at the start of the specified cell. Arguments
   * are not checked.
   * <pre><code>
   *   int position = cellIndex * cellbytes;
   *   int limit = position + cellbytes;
   *   buffer.limit(limit).position(position);
   * </code></pre>
   * 
   * 
   * @param cellIndex
   *        zero based cell index (starts at the beginning of the <code>buffer</code>)
   * @param cellbytes
   *        number of bytes in each buffer
   * @return
   *        <code>buffer</code>
   */
  public static ByteBuffer setCell(ByteBuffer buffer, int cellIndex, int cellbytes) {
    int position = cellIndex * cellbytes;
    int limit = position + cellbytes;
    buffer.limit(limit).position(position);
    return buffer;
  }
  
  
  /**
   * Returns a new, read-only slice of the given buffer with a few optimizations
   * that are otherwise repetitive. The one exception to <em>new</em> is when the
   * buffer has no remain bytes: in that event the stateless singleton {@linkplain #NULL_BUFFER}
   * is returned.
   */
  public static ByteBuffer readOnlySlice(ByteBuffer buffer) {
    if (!buffer.hasRemaining())
      return NULL_BUFFER;
    
    if (!buffer.isReadOnly())
      buffer = buffer.asReadOnlyBuffer();
    
    return buffer.slice();
  }
  
  
  /**
   * Slices out the given number of number of {@code bytes} from the {@code buffer}
   * and returns it. On return, the buffer's position is advanced by as many bytes.
   */
  public static ByteBuffer slice(ByteBuffer buffer, int bytes) {
    
    //bounds check
    if (bytes <= 0) {
      if (bytes == 0)
        return NULL_BUFFER;
      
      throw new IllegalArgumentException("negative bytes " + bytes);
    
    } else if (bytes > buffer.remaining()) {
      throw new IllegalArgumentException(
          "bytes " + bytes + " > remaining in buffer " + buffer);
    }
    
    int limit = bytes + buffer.position();
    int savedLimit = buffer.limit();
    
    ByteBuffer slice = buffer.limit(limit).slice();
    buffer.position(limit).limit(savedLimit);
    return slice;
  }
  
  
  
  /**
   * Reads the given input stream fully and returns the bytes read as a
   * {@code ByteBuffer}.
   */
  public static ByteBuffer readFully(InputStream in) throws UncheckedIOException {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] bytes = new byte[4096];
      int amount;
      while ((amount = in.read(bytes)) != -1)
        out.write(bytes, 0, amount);
      return ByteBuffer.wrap(out.toByteArray());
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }

  
  
  /**
   * Returns the first index of the specified <code>key</code>, assuming the <code>cells</code> are sorted in
   * ascending <code>order</code>. Results are undefined otherwise. If no matching cell is found,
   * then the [negative] index returned has the same semantics as that specified in
   * {@linkplain Arrays#binarySearch(Object[], Object, Comparator)}.
   * 
   * @param order
   *        if <code>null</code>, then searched in lexical order
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
   * Returns the last index of the specified <code>key</code>, assuming the <code>cells</code> are sorted in
   * ascending <code>order</code>. Results are undefined otherwise. If no matching cell is found,
   * then the [negative] index returned has the same semantics as that specified in
   * {@linkplain Arrays#binarySearch(Object[], Object, Comparator)}.
   * 
   * @param order
   *        if <code>null</code>, then searched in lexical order
   * 
   * @throws NullPointerException
   *         if <code>key</code>, <code>cells</code>, or any of its elements is <code>null</code>
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
   * <p>
   * Binary searches for the specified <code>key</code>, assuming the <code>cells</code> are sorted in
   * ascending <code>order</code>. Results are undefined otherwise. If no matching cell is found,
   * then the [negative] index returned has the same semantics as that specified in
   * {@linkplain Arrays#binarySearch(Object[], Object, Comparator)}.
   * </p><p>
   * This method behaves exactly as {@linkplain #binaryFirst(ByteBuffer[], ByteBuffer, Comparator) binaryFirst} and
   * {@linkplain #binaryLast(ByteBuffer[], ByteBuffer, Comparator) binaryLast}, if there are no duplicate
   * cells (except that its faster); if there are dups, and there is a hit on a dupe,
   * then there is no guarantee which of the dups is chosen.
   * </p>
   * 
   * @param order
   *        if <code>null</code>, then searched in lexical order
   * @return {@code java.util.Arrays.binarySearch(cells, key, order)}
   * 
   * @throws NullPointerException
   *         if <code>key</code>, <code>cells</code>, or any of its elements is <code>null</code>
   */
  public static int binarySearch(ByteBuffer[] cells, ByteBuffer key, Comparator<ByteBuffer> order) {
    return Arrays.binarySearch(cells, key, order);
  }
  
  

  
  
  /**
   * Tests whether the cells are currently sorted.
   * 
   * @param order
   *        if <code>null</code>, then searched in lexical order
   * 
   * @throws NullPointerException
   *         if <code>cells</code> or any of its elements is <code>null</code>
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
   * Convenience method to clear array of <code>buffers</code>.
   * 
   * @throws NullPointerException
   *         if <code>buffer</code> or any of its elements is <code>null</code>
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
