/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.gnahraf.io.store.table.del.DeleteCodec;

/**
 * 
 * @author Babak
 */
public class TableSetDIterator extends TableSetIterator {
  
  private final DeleteCodec deleteCodec;
  
  
  public TableSetDIterator(TableSetD tableSet, ByteBuffer key) throws IOException {
    super(tableSet, key);
    deleteCodec = tableSet.getDeleteCodec();
  }


  @Override
  protected ByteBuffer nextImpl(ByteBuffer next) throws IOException {
    while ((next = super.nextImpl(next)) != null && deleteCodec.isDeleted(next));
    return next;
  }

}
