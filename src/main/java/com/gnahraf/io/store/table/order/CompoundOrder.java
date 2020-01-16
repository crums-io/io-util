/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.order;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Compound ordering.
 * 
 * @author Babak
 */
public final class CompoundOrder extends RowOrder {
  
  /**
   * Stack of row orders. Precedence is from back to front.
   */
  private final RowOrder[] orders;

  
  /**
   * Creates a new instance with the given array of <tt>RowOrder</tt>s.
   * The precedence of the orderings is from front to back (first to last
   * index in the array).
   * 
   * @param orders
   *        non-<tt>null</tt> array of row orders of length &gt; 0. On
   *        return, you can do whatever with array: a defensive copy is
   *        maintained by this instance.
   */
  public CompoundOrder(RowOrder[] orders) {
    if (orders == null || orders.length < 1)
      throw new IllegalArgumentException("empty orders: " + orders);
    RowOrder[] copy = new RowOrder[orders.length];
    for (int i = orders.length, j = 0; i-- > 0; ++j) {
      copy[j] = orders[i];
      if (orders[i] == null)
        throw new IllegalArgumentException("null order at index " + i);
    }
    this.orders = copy;
  }


  @Override
  public int compareRows(ByteBuffer rowA, ByteBuffer rowB) {
    for (int i = orders.length; i-- > 0; ) {
      int comp = orders[i].compareRows(rowA, rowB);
      if (comp != 0)
        return comp;
    }
    return 0;
  }
  
  
  @Override
  public boolean equals(Object other) {
    if (other == this)
      return true;
    else if (other instanceof CompoundOrder) {
      CompoundOrder otherCompound = (CompoundOrder) other;
      return Arrays.equals(orders, otherCompound.orders);
    } else
      return false;
  }
  
  
  @Override
  public int hashCode() {
    return Arrays.hashCode(orders);
  }

}
