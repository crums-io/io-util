/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.iter;


import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An directed iterator of fixed-width rows.
 */
public abstract class RowIterator {
  
  /**
   * Returns the direction of the iteration. If {@linkplain Direction#FORWARD FORWARD},
   * then successive non-<code>null</code> rows returned by this instance are monotonically
   * increasing; if {@linkplain Direction#REVERSE REVERSE}, then successive
   * non-<code>null</code> rows returned by this instance are monotonically
   * decreasing.
   */
  public abstract Direction getDirection();
  
  /**
   * Returns the number of bytes in each row.
   */
  public abstract int getRowWidth();
  
  /**
   * Returns the next row, or <code>null</code> if at the end of the iteration. Successive
   * invocations return monotonically increasing or decreasing rows depending on the
   * instance's {@linkplain #getDirection() direction}.
   * 
   * @return
   *        a buffer loaded with the contents of the next row (flipped and ready
   *        to read), or <code>null</code> if at the end of the iteration. The returned
   *        buffer may be read-only.
   *        
   * @see #next(ByteBuffer)
   */
  public abstract ByteBuffer next() throws IOException;
  
  /**
   * Returns the next row, or <code>null</code> if at the end of the iteration. This method
   * behaves exactly like {@linkplain #next()} except it uses the given work buffer. Successive
   * invocations return monotonically increasing or decreasing rows depending on the
   * instance's {@linkplain #getDirection() direction}.
   * 
   * @param buffer
   *        the buffer the contents of the row is written to. Its capacity must be at least
   *        {@linkplain #getRowWidth() rowWidth} bytes.
   * @return
   *        the given buffer loaded with the contents of the next row (flipped and ready
   *        to read), or <code>null</code> if at the end of the iteration. The base implementation
   *        just calls {@linkplain #next()} and copies the contents into the given buffer. If
   *        a subclass can optimize this better, it should.
   * @throws IllegalArgumentException
   *        if the capacity of <code>buffer</code> is less than the {@linkplain #getRowWidth() rowWidth}
   * @see #next()
   */
  public ByteBuffer next(ByteBuffer buffer) throws IOException {
    if (buffer == null)
      throw new IllegalArgumentException("null buffer");
    if (buffer.capacity() < getRowWidth())
      throw new IllegalArgumentException(
          "insufficient buffer capacity: expected at least " + getRowWidth() +
          " but actual was " + buffer.capacity() + ". buffer: " + buffer);
    ByteBuffer next = next();
    if (next == null)
      return null;
    buffer.clear();
    buffer.put(next).flip();
    return buffer;
  }

}
