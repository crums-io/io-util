/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.order;

import java.nio.ByteBuffer;

/**
 * 
 * @author Babak
 */
public class RowOrders {

  private RowOrders() { }

  
  public final static RowOrder SHORT_ORDER = shortOrderAtOffset(0);
  
  public final static RowOrder INT_ORDER = intOrderAtOffset(0);
  
  public final static RowOrder LONG_ORDER = longOrderAtOffset(0);
  
  
  public static RowOrder intOrderAtOffset(int offset) {
    return new RowOrderAtOffset(offset) {
      @Override
      public int compareRows(ByteBuffer rowA, ByteBuffer rowB) {
        int a = rowA.getInt(offset);
        int b = rowB.getInt(offset);
        if (a > b)
          return 1;
        else if (a == b)
          return 0;
        else
          return -1;
      }
    };
  }
  
  
  public static RowOrder longOrderAtOffset(int offset) {
    return new RowOrderAtOffset(offset) {
      @Override
      public int compareRows(ByteBuffer rowA, ByteBuffer rowB) {
        long a = rowA.getLong(offset);
        long b = rowB.getLong(offset);
        if (a > b)
          return 1;
        else if (a == b)
          return 0;
        else
          return -1;
      }
    };
  }
  
  
  public static RowOrder shortOrderAtOffset(int offset) {
    return new RowOrderAtOffset(offset) {
      @Override
      public int compareRows(ByteBuffer rowA, ByteBuffer rowB) {
        int a = rowA.getShort(offset);
        int b = rowB.getShort(offset);
        return a - b;
      }
    };
  }


}
