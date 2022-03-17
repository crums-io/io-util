/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.util;


import static org.junit.jupiter.api.Assertions.*;

import java.util.function.Function;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class CacheWheelTest {
  
  @Test
  public void testTenSlot() {
    int maxSize = 1000;
    int slots = 10;
    CacheWheel<String> wheel = new CacheWheel<>(new String[slots], String::valueOf, maxSize);
    
    assertRange(wheel, 6, 35);
    assertRange(wheel, 35, 6);
  }
  
  
  
  
  private void assertRange(CacheWheel<String> wheel, int from, int to) {
    var range = Lists.intRange(from, to);
    if (from > to)
      range = Lists.reverse(range);
    range.forEach(i -> assertValue(wheel, i));
  }
  
  
  
  private void assertValue(CacheWheel<String> wheel, int index) {
    assertEquals(String.valueOf(index), wheel.get(index));
  }
  
  
  
  
  @Test
  public void testTenSlotWithCacheAssertions() {
    int maxSize = 1000;
    int slots = 10; // don't change this
    
    var factory = new BlowupFactory();
    
    CacheWheel<String> wheel = new CacheWheel<>(new String[slots], factory, maxSize);
    
    assertRange(wheel, 6, 35);
    
    factory.setCached(true);
    // test the test
    try {
      assertValue(wheel, 25);
      fail();
    } catch (RuntimeException expected) {  }
    
    // but after failing (at the factory), the wheel's state
    // must still be good..
    assertRange(wheel, 35, 26);
    assertRange(wheel, 26, 35);
    
    factory.setCached(false);
    assertRange(wheel, 957, 900);

    factory.setCached(true);
    assertRange(wheel, 900, 909);

    factory.setCached(false);
    assertRange(wheel, 802, 800);

    factory.setCached(true);
    assertRange(wheel, 802, 800);
  }
  
  
  
  private static class BlowupFactory implements Function<Integer, String> {
    
    private boolean blow;

    @Override
    public String apply(Integer t) {
      if (blow)
        throw new RuntimeException("unexpected access on index " + t);
      return t.toString();
    }
    
    void setCached(boolean cached) {
      this.blow = cached;
    }
    
  }

}
