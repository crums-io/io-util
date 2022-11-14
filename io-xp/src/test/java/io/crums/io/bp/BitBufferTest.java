/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.io.bp;


import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class BitBufferTest {

  @Test
  public void testEmpty() {
    var bitBuffer = new BitBuffer(16);
    assertEquals(0, bitBuffer.bitPosition());
    assertEquals(0, bitBuffer.byteLength());
    assertFalse(bitBuffer.asByteBuffer().hasRemaining());
    bitBuffer.clear();
  }
  
  
  @Test
  public void testOneBitLeft() {
    var buffer = new BitBuffer(16);
    byte b = (byte) 0b10000000;
    buffer.putLeft(b, 1);
    var byteBuffer = buffer.asByteBuffer();
    assertEquals(1, byteBuffer.remaining());
    assertEquals(b, byteBuffer.get());
    buffer.clear();
    assertEquals(0, buffer.bitPosition());
    assertEquals(0, buffer.byteLength());
    assertFalse(buffer.asByteBuffer().hasRemaining());
    b = 0;
    buffer.putLeft(b, 1);
    byteBuffer = buffer.asByteBuffer();
    assertEquals(1, byteBuffer.remaining());
    assertEquals(b, byteBuffer.get());
  }
  
  @Test
  public void testOneBitRight() {
    var buffer = new BitBuffer(16);
    byte expected = (byte) 0b1000_0000;
    buffer.putRight((byte) 1, 1);
    var byteBuffer = buffer.asByteBuffer();
    assertEquals(1, byteBuffer.remaining());
    assertEquals(expected, byteBuffer.get());
  }
  
  
  @Test
  public void testTwoBits() {
    var buffer = new BitBuffer(16);
    byte expected = (byte) 0b0100_0000;
    buffer.putRight((byte) 0, 1);
    buffer.putRight((byte) 1, 1);
    var byteBuffer = buffer.asByteBuffer();
    assertEquals(1, byteBuffer.remaining());
    assertEquals(expected, byteBuffer.get());
    
    buffer.clear();
    expected = (byte) 0b11000000;
    buffer.putRight((byte) 1, 1);
    buffer.putRight((byte) 1, 1);
    byteBuffer = buffer.asByteBuffer();
    assertEquals(1, byteBuffer.remaining());
    assertEquals(expected, byteBuffer.get());
  }
  
  
  @Test
  public void testNineBits() {
    var buffer = new BitBuffer(16);
    // expected bytes
    byte exp0 = (byte) 0b1101_0010;
    byte exp1 = (byte) 0b1000_0000;
    
    buffer.putLeft((byte) 0b1101_0000, 5);
    buffer.putLeft((byte) 0b0101_0000, 4);
    
//    var expected = ByteBuffer.allocate(2).put(exp0).put(exp1).flip();
    var byteBuffer = buffer.asByteBuffer();
    assertEquals(2, byteBuffer.remaining());
    assertEquals(exp0, byteBuffer.get());
    assertEquals(exp1, byteBuffer.get());
    
    buffer.clearFullBytes();
    assertEquals(1, buffer.bitPosition());
  }
  
  
  
  
  private void testMany(byte[] data, int lastBits) {
    var buffer = new BitBuffer(8 + data.length);
  }

}
















