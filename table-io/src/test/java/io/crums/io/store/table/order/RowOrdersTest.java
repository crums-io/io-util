/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.order;


import static io.crums.io.store.table.order.RowOrders.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

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
