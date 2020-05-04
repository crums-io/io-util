/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.order;

import java.nio.ByteBuffer;

import com.gnahraf.io.store.table.del.DeleteCodec;


/**
 * Uses the natural order of rows.
 * <p>
 * <em>TODO: Remove. It's a bad design since it cannot work alongside a {@linkplain DeleteCodec}.
 * </p>
 * 
 * @author Babak
 */
public final class NaturalRowOrder extends RowOrder {
  
  public final static NaturalRowOrder INSTANCE = new NaturalRowOrder();

  @Override
  public int compareRows(ByteBuffer rowA, ByteBuffer rowB) {
    return rowA.compareTo(rowB);
  }

}
