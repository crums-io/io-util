/*
 * Copyright 2019 Babak Farhang
 */
package io.crums.util;


import static io.crums.util.IntegralStrings.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Random;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class IntegralStringsTest {
  
  @Test
  public void testIsHex() {
    String[] hex = { "fE", "00", "10", "0123456789abcdefABCDEF" };
    String[] notHex = { "0x5", "5G", "0g" };
    for (String n : hex)
      assertTrue(isHex(n), n);
    for (String g : notHex)
      assertFalse(isHex(g), g);
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
