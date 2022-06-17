/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.util;


import static org.junit.jupiter.api.Assertions.*;

import java.util.Comparator;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class PrimitiveComparatorTest {
  
  private final Comparator<Number> comparator = PrimitiveComparator.INSTANCE;
  
  @Test
  public void testMaxLong() {
    assertOrder(Long.MAX_VALUE - 1, Long.MAX_VALUE);
  }
  
  
  @Test
  public void testMinLong() {
    assertOrder(Long.MIN_VALUE, Long.MIN_VALUE + 1);
  }
  
  
  @Test
  public void testLongAndDouble() {
    assertSame(1L, 1.0);
  }
  
  
  @Test
  public void testFloatAndDouble() {
    assertSame(0.125, (float) 0.125);
    assertSame(1.125, (float) 1.125);
  }
  
  
  private void assertSame(Number a, Number b) {
    assertEquals(0, comparator.compare(a, b));
  }
  
  private void assertOrder(Number first, Number second) {
    assertTrue(comparator.compare(first, second) < 0);
    assertTrue(comparator.compare(second, first) > 0, "reflexive property failure");
  }

}
