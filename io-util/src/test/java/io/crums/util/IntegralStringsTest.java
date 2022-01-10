/*
 * Copyright 2019 Babak Farhang
 */
package io.crums.util;


import static io.crums.util.IntegralStrings.*;
import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;

/**
 * 
 */
public class IntegralStringsTest {
  
  @Test
  public void testIsHex() {
    String[] hex = { "fE", "00", "10", "0123456789abcdefABCDEF" };
    String[] notHex = { "0x5", "5G", "0g" };
    for (String n : hex)
      assertTrue(n, isHex(n));
    for (String g : notHex)
      assertFalse(g, isHex(g));
  }
  
  
  @Test
  public void testRoundtripHex() {
    byte[] bytes = new byte[256];
    new Random(22).nextBytes(bytes);
    String hex = toHex(bytes);
    byte[] out = hexToBytes(hex);
    assertArrayEquals(bytes, out);
  }

}
