/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.iter;


import java.io.IOException;
import java.nio.ByteBuffer;

import io.crums.io.store.table.del.DeleteCodec;

/**
 * Filters out deleted rows.
 * 
 * @author Babak
 */
public class RowIteratorD extends FilterRowIterator {
  
  protected final DeleteCodec deleteCodec;

  public RowIteratorD(RowIterator impl, DeleteCodec deleteCodec) {
    super(impl);
    this.deleteCodec = deleteCodec;
    
    if (deleteCodec == null)
      throw new IllegalArgumentException("null deleteCodec");
  }

  @Override
  public ByteBuffer next() throws IOException {
    ByteBuffer next;
    while ((next = impl.next()) != null && deleteCodec.isDeleted(next));
    return next;
  }

  @Override
  public ByteBuffer next(ByteBuffer buffer) throws IOException {
    while ((buffer = impl.next(buffer)) != null && deleteCodec.isDeleted(buffer));
    return buffer;
  }

}
