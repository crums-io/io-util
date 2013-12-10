/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.iter;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Base wrapper implementation around an existing <tt>RowIterator</tt>.
 * 
 * @author Babak
 */
public class FilterRowIterator extends RowIterator {
  
  /**
   * The underlying implementation.
   */
  protected final RowIterator impl;
  
  
  /**
   * Creates a new instance with the given underlying implementation.
   */
  public FilterRowIterator(RowIterator impl) {
    if (impl == null)
      throw new IllegalArgumentException("null impl");
    this.impl = impl;
  }
  



  @Override
  public Direction getDirection() {
    return impl.getDirection();
  }


  @Override
  public int getRowWidth() {
    return impl.getRowWidth();
  }


  @Override
  public ByteBuffer next() throws IOException {
    return impl.next();
  }
  
  @Override
  public ByteBuffer next(ByteBuffer buffer) throws IOException {
    return impl.next(buffer);
  }

}
