/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.iter;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.crums.io.store.table.TableSetD;
import io.crums.io.store.table.del.DeleteCodec;

/**
 * 
 * @author Babak
 */
public class TableSetDIterator extends TableSetIterator {
  
  private final DeleteCodec deleteCodec;
  
  
  public TableSetDIterator(TableSetD tableSet) throws IOException {
    super(tableSet);
    deleteCodec = tableSet.getDeleteCodec();
  }


  @Override
  protected ByteBuffer nextImpl(ByteBuffer next) throws IOException {
    while ((next = super.nextImpl(next)) != null && deleteCodec.isDeleted(next));
    return next;
  }

}
