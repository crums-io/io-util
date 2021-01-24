/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util;


import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;

import com.gnahraf.test.SelfAwareTestCase;

/**
 * 
 */
public class Base64_32Test extends SelfAwareTestCase {
  
  
  @Test
  public void test000() {
    byte[] value = new byte[32];
    testImpl(value, new Object() { });
  }
  
  @Test
  public void testOne() {
    byte[] value = new byte[32];
    final Object label = new Object() {  };
    Random random = new Random(99);
    random.nextBytes(value);
    testImpl(value, label);
  }
  
  @Test
  public void testFew() {
    int count = 12;
    byte[] value = new byte[32];
    final Object label = new Object() {  };
    Random random = new Random(count);
    for (int countdown = count; countdown-- > 0; ) {
      random.nextBytes(value);
      testImpl(value, label);
    }
  }
  
  @Test
  public void test100() {
    int count = 100;
    byte[] value = new byte[32];
//    final Object label = new Object() {  };
    Random random = new Random(count);
    for (int countdown = count; countdown-- > 0; ) {
      random.nextBytes(value);
//      testImpl(value, label);
      testImpl(value, null);
    }
  }
  
  @Test
  public void test1M() {
    int count = 1_000_000;
    byte[] value = new byte[32];
    Random random = new Random(count);
    for (int countdown = count; countdown-- > 0; ) {
      random.nextBytes(value);
      testImpl(value, null);
    }
  }
  
  
  private void testImpl(byte[] value, Object label) {
    String base64 = Base64_32.encode(value);
    if (label != null) {
      String method = method(label);
      System.out.println(method + " b64: " + base64);
      System.out.println(method + " hex: " + IntegralStrings.toHex(value));
    }
    byte[] out = Base64_32.decode(base64);
    assertArrayEquals(value, out);
  }

}
