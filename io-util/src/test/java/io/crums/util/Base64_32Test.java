/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util;


import static org.junit.Assert.*;

import java.nio.ByteBuffer;
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
  public void testEncodeNext32() {
    byte[] value = new byte[64];
    Random random = new Random(11);
    random.nextBytes(value);
    String[] encoded = new String[2];
    ByteBuffer buffer = ByteBuffer.wrap(value);
    encoded[0] = Base64_32.encodeNext32(buffer);
    encoded[1] = Base64_32.encodeNext32(buffer);
    assertFalse(buffer.hasRemaining());
    ByteBuffer decoded = ByteBuffer.allocate(64);
    for (var enc : encoded)
      decoded.put(Base64_32.decode(enc));
    
    assertFalse(decoded.hasRemaining());
    decoded.flip();
    
    assertEquals(buffer.clear(), decoded);
  }
  
  @Test
  public void testLoByte() {
    System.out.println();
    final Object label = new Object() {  };
    
    System.out.println(method(label) + ": testing all 256 combinations");
    System.out.println();
    
    byte[] value = new byte[32];
    for (int i = 0; i < 256; ++i) {
      Object lb;
      {
        int edgeMod = i % 64;
        lb = (edgeMod < 3 || edgeMod > 60) ? label : null;
      }
      value[31] = (byte) i;
      testImpl(value, lb);
    }
  }
  
  @Test
  public void testHiByte() {
    System.out.println();
    final Object label = new Object() {  };
    
    System.out.println(method(label) + ": testing all 256 combinations");
    System.out.println();
    
    byte[] value = new byte[32];
    for (int i = 0; i < 256; ++i) {
      Object lb;
      {
        int edgeMod = i % 64;
        lb = (edgeMod < 3 || edgeMod > 60) ? label : null;
      }
      value[0] = (byte) i;
      testImpl(value, lb);
    }
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
