/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.order;

import java.nio.ByteBuffer;

/**
 * Utility for creating common {@linkplain RowOrder}s. Note the orderings
 * defined here do not consider a buffer's position: they see the
 * buffer in absolute positions.
 * <p>
 * This is not necessarily a good thing. The
 * natural (lexical) ordering of <code>ByteBuffer</code> for example, considers
 * the ordering relative to the buffer's current position. On the plus side,
 * with such orderings, you don't at least need to worry a buffer's positional
 * state.
 * </p>
 */
public class RowOrders {

  private RowOrders() { }


  /**
   * Short order at zero offset.
   */
  public final static RowOrder SHORT_ORDER = shortOrderAtOffset(0);
  
  /**
   * Int order at zero offset.
   */
  public final static RowOrder INT_ORDER = intOrderAtOffset(0);

  /**
   * Long order at zero offset.
   */
  public final static RowOrder LONG_ORDER = longOrderAtOffset(0);
  
  
  /**
   * Returns 4 byte <em>int</em> order at byte <code>offset</code>.
   */
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
      @Override
      public String toString() {
        return "[INT_ORDER, off=" + offset + "]";
      }
    };
  }
  

  /**
   * Returns 8 byte <em>long</em> order at byte <code>offset</code>.
   */
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
      @Override
      public String toString() {
        return "[LONG_ORDER, off=" + offset + "]";
      }
    };
  }
  

  /**
   * Returns 2 byte <em>short</em> order at byte <code>offset</code>.
   */
  public static RowOrder shortOrderAtOffset(int offset) {
    return new RowOrderAtOffset(offset) {
      @Override
      public int compareRows(ByteBuffer rowA, ByteBuffer rowB) {
        int a = rowA.getShort(offset);
        int b = rowB.getShort(offset);
        return a - b;
      }
      @Override
      public String toString() {
        return "[SHORT_ORDER, off=" + offset + "]";
      }
    };
  }


}
