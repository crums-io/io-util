/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.order;

import java.nio.ByteBuffer;


/**
 * 
 * @author Babak
 */
public final class LexicalRowOrder extends RowOrder {
  
  public final static LexicalRowOrder INSTANCE = new LexicalRowOrder();

  @Override
  public int compareRows(ByteBuffer rowA, ByteBuffer rowB) {
    return rowA.compareTo(rowB);
  }

  /**
   * @return <tt>true</tt>
   */
  @Override
  public boolean isRelative() {
    return true;
  }

}
