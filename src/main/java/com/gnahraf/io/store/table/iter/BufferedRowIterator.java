/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.iter;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A read-ahead <tt>RowIterator</tt>. The buffering here has nothing to do with
 * performance optimization: if anything, this implementation is <em>less</em>
 * efficient.
 * 
 * @see #peek()
 * 
 * @author Babak
 */
public class BufferedRowIterator extends FilterRowIterator {
  
  private ByteBuffer currentRow;

  /**
   * Constructs a new instance by pre-loading the next row.
   */
  public BufferedRowIterator(RowIterator impl) throws IOException {
    super(impl);
    currentRow = impl.next();
  }


  @Override
  public ByteBuffer next() throws IOException {
    if (currentRow == null)
      return null;
    
    ByteBuffer next = currentRow;
    currentRow = impl.next();
    return next;
  }
  
  /**
   * Peeks at the next row without advancing it. If the caller modifies a returned
   * buffer (even by changing its position), the modifications will survive
   * a subsequent call to {@linkplain #next()}. So it's best not to modify the returned
   * buffer (if any).
   * 
   * @return the next row, or <tt>null</tt>, if at the end of the sequence.
   */
  public ByteBuffer peek() {
    return currentRow;
  }
  
  
  /**
   * Tells whether there is a next row.
   */
  public boolean hasNext() {
    return peek() != null;
  }


  @Override
  public ByteBuffer next(ByteBuffer buffer) throws IOException {
    if (buffer == null)
      throw new IllegalArgumentException("null buffer");
    if (buffer.capacity() < getRowWidth())
      throw new IllegalArgumentException("buffer cap < row width (" + getRowWidth() + "): " + buffer);
    ByteBuffer next = next();
    buffer.clear();
    buffer.put(next).flip();
    return buffer;
  }

}
