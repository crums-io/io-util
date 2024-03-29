/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.iter;


import static io.crums.io.store.table.iter.Direction.FORWARD;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableSet;

/**
 * A <code>RowIterator</code> implementation over an in-memory sorted set of buffers.
 */
public class SortedBufferIterator extends RowIterator {
  
  protected final Iterator<ByteBuffer> iter;
  private final Direction direction;
  private final int rowWidth;
  
  /**
   * Creates an instance using the given set of <code>buffers</code>, with the given <code>direction</code>
   * and specified <code>rowWidth</code>.
   * 
   * @param buffers
   *        the ordered set of buffers. The members of this set are expected to be <code>ByteBuffer</code>s
   *        with <code>rowWidth</code> remaining bytes. The constructor does not validate this condition.
   * @param direction
   *        {@linkplain Direction#FORWARD FORWARD} or {@linkplain Direction#REVERSE REVERSE}
   * @param rowWidth
   *        the number of bytes in a row
   */
  public SortedBufferIterator(NavigableSet<ByteBuffer> buffers, Direction direction, int rowWidth) {
    
    if (buffers == null)
      throw new IllegalArgumentException("null buffer set");
    if (direction == null)
      throw new IllegalArgumentException("null direction");
    if (rowWidth < 1)
      throw new IllegalArgumentException("rowWidth: " + rowWidth);
    
    this.iter = direction == FORWARD ? buffers.iterator() : buffers.descendingIterator();
    this.direction = direction;
    this.rowWidth = rowWidth;
  }


  @Override
  public final Direction getDirection() {
    return direction;
  }


  @Override
  public final int getRowWidth() {
    return rowWidth;
  }


  @Override
  public ByteBuffer next() {
    if (!iter.hasNext())
      return null;
    return iter.next().asReadOnlyBuffer();
  }


  @Override
  public ByteBuffer next(ByteBuffer buffer) {
    if (buffer.capacity() < rowWidth)
      throw new IllegalArgumentException("buffer capacity too small (min " + rowWidth + "): " + buffer);
    if (!iter.hasNext())
      return null;
    ByteBuffer next = iter.next();
    // (concurrent access failure points below..)
    buffer.clear();
    next.mark();
    buffer.put(next);
    next.reset();
    buffer.flip();
    return buffer;
  }

}
