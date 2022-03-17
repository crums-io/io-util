/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util;


import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class BigShortTest {

  
  @Test
  public void testRoundtrips() {
    int[] bigshorts = {
        0, 1, 0xff, 256, 257, 0xfff, 0x1000, 0x1005, 0xffff, 0x10000, 0x100e02,
        0xfffffe, 0xffffff,
    };
    for (int bigshort : bigshorts)
      testRoundtrip(bigshort);
  }
  
  
  @Test
  public void testOutOfBounds() {
    try {
      new BigShort(-1);
      fail();
    } catch (IllegalArgumentException expected) {    }
    
    int oneTooBig = 0x1000000;
    assertEquals(oneTooBig, BigShort.MAX_VALUE + 1);
    try {
      new BigShort(oneTooBig);
      fail();
    } catch (IllegalArgumentException expected) {    }
  }
  
  
  private void testRoundtrip(int bigshort) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    BigShort.putBigShort(buffer, bigshort);
    assertEquals(3, buffer.position());
    int out = BigShort.getBigShort(buffer.flip());
    assertEquals(bigshort, out);
  }

}
