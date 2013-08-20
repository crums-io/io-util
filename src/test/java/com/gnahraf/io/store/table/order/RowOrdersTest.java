/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.order;


import static org.junit.Assert.*;
import static com.gnahraf.io.store.table.order.RowOrders.*;

import org.junit.Test;

/**
 * 
 * @author Babak
 */
public class RowOrdersTest {

  @Test
  public void testEquivalence1() {
    assertEquivalent(INT_ORDER, intOrderAtOffset(0));
  }
  


  @Test
  public void testEquivalence2() {
    assertEquivalent(intOrderAtOffset(0), intOrderAtOffset(0));
  }

  @Test
  public void testEquivalence3() {
    assertFalse(intOrderAtOffset(0).equals(intOrderAtOffset(1)));
  }
  
  
  private void assertEquivalent(Object o1, Object o2) {
    assertFalse(o1 == o2);
    assertTrue(o1.equals(o2));
  }

}
