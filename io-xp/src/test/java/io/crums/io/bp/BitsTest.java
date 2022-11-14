/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.io.bp;


import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.function.BiConsumer;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class BitsTest {

  @Test
  public void testShiftEmpty() {
    byte[] empty = new byte[0];
    Bits.shiftLeft(empty, 0);
    Bits.shiftLeft(empty, 1);
    Bits.shiftLeft(empty, 8);
    Bits.shiftRight(empty, 0);
    Bits.shiftRight(empty, 1);
    Bits.shiftRight(empty, 8);
    
    assertIllegalArg(Bits::shiftLeft, empty, 9);
    assertIllegalArg(Bits::shiftLeft, empty, -1);
    assertIllegalArg(Bits::shiftRight, empty, 9);
    assertIllegalArg(Bits::shiftRight, empty, -1);
  }
  
  
  private void assertIllegalArg(BiConsumer<byte[], Integer> func, byte[] array, int bits) {
    try {
      func.accept(array, bits);
      fail();
    } catch (IllegalArgumentException expected) {  }
  }
  
  
  @Test
  public void testPatchBits() {
    assertPatchBits(0b1010_1010, 0b1101_1101, 0, 0b1010_1010);
    assertPatchBits(0b1010_1010, 0b1101_1101, 1, 0b010_1010_1);
    assertPatchBits(0b1010_1010, 0b1101_1101, 2, 0b10_1010_11);
    assertPatchBits(0b1010_1010, 0b1101_1101, 3, 0b0_1010_110);
    assertPatchBits(0b1010_1010, 0b1101_1101, 4, 0b1010_1101);
    assertPatchBits(0b1010_1010, 0b1101_1101, 5, 0b010_1101_1);
    assertPatchBits(0b1010_1010, 0b1101_1101, 6, 0b10_1101_11);
    assertPatchBits(0b1010_1010, 0b1101_1101, 7, 0b0_1101_110);
    assertPatchBits(0b1010_1010, 0b1101_1101, 8, 0b1101_1101);
    
    
    try {
      Bits.patchBits((byte) 0,  (byte) 0, -1);  fail();
    } catch (IllegalArgumentException expected) {  }
    try {
      Bits.patchBits((byte) 0,  (byte) 0, 9);   fail();
    } catch (IllegalArgumentException expected) {  }
  }
  
  
  private void assertPatchBits(int a, int b, int bits, int expected) {
    assertEquals((byte) expected, Bits.patchBits((byte) a, (byte) b, bits));
  }
  
  
  
  @Test
  public void testShift1() {
    byte[] array = new byte[1];
    byte[] expected = new byte[1];
    // test it doesn't do anything weird with zeroes..
    for (int shift = 0; shift <= 8; ++shift) {
      Bits.shiftLeft(array, shift);
      assertArrayEquals(expected, array);
      Bits.shiftRight(array, shift);
      assertArrayEquals(expected, array);
    }
    
    array[0] =    (byte) 0b0010_0110;
    Bits.shiftLeft(array, 1);
    expected[0] = (byte) 0b010_0110_0;
    assertArrayEquals(expected, array);
    
    array[0] =    (byte) 0b0010_0110;
    Bits.shiftRight(array, 1);
    expected[0] = (byte) 0b0_0010_011;
    assertArrayEquals(expected, array);
  }
  
  
  @Test
  public void testShift2() {
    byte[] start = new byte[] {
        (byte) 0b1011_1011,
        (byte) 0b0101_0101
    };
    testShift2Left(start, 0, 0b1011_1011, 0b0101_0101);
    testShift2Left(start, 1, 0b011_1011_0, 0b101_0101_0);
    testShift2Left(start, 2, 0b11_1011_01, 0b01_0101_00);
    testShift2Left(start, 3, 0b1_1011_010, 0b1_0101_000);
    testShift2Left(start, 4, 0b1011_0101, 0b0101_0000);
    testShift2Left(start, 5, 0b011_0101_0, 0b101_0000_0);
    testShift2Left(start, 6, 0b11_0101_01, 0b01_0000_00);
    testShift2Left(start, 7, 0b1_0101_010, 0b1_0000_000);
    testShift2Left(start, 8, 0b0101_0101, 0b0000_0000);
    

    testShift2Right(start, 0, 0b1011_1011, 0b0101_0101);
    testShift2Right(start, 1, 0b0_1011_101, 0b1_0101_010);
    testShift2Right(start, 2, 0b00_1011_10, 0b11_0101_01);
    testShift2Right(start, 3, 0b000_1011_1, 0b011_0101_0);
    testShift2Right(start, 4, 0b0000_1011, 0b1011_0101);
    testShift2Right(start, 5, 0b0_0000_101, 0b1_1011_010);
    testShift2Right(start, 6, 0b00_0000_10, 0b11_1011_01);
    testShift2Right(start, 7, 0b000_0000_1, 0b011_1011_0);
    testShift2Right(start, 8, 0b0000_0000, 0b1011_1011);
  }
  
  
  private void testShift2Left(byte[] start, int shift, int exp0, int exp1) {
    testShift2(start, shift, exp0, exp1, true);
  }
  
  private void testShift2Right(byte[] start, int shift, int exp0, int exp1) {
    testShift2(start, shift, exp0, exp1, false);
  }
  
  private void testShift2(byte[] start, int shift, int exp0, int exp1, boolean left) {
    byte[] array = start.clone();
    
    if (left)
      Bits.shiftLeft(array, shift);
    else
      Bits.shiftRight(array, shift);
    
    assertEquals((byte) exp0, array[0]);
    assertEquals((byte) exp1, array[1]);
    
  }
  
  
  @Test
  public void testShift3() {
    byte[] start = {
        (byte) 0b1010_1010,
        (byte) 0b1100_1100,
        (byte) 0b0111_0111,
    };
    
    int shift = 0;
    int[] expected = {
        0b1010_1010,
        0b1100_1100,
        0b0111_0111,
    };
    
    testShiftLeft(start, shift, expected);
    testShiftRight(start, shift, expected);
    
    shift = 8; // left
    expected = new int[] {
        0b1100_1100,
        0b0111_0111,
        0,
    };
    testShiftLeft(start, shift, expected);
    
    shift = 8; // right
    expected = new int[] {
        0,
        0b1010_1010,
        0b1100_1100,
    };
    testShiftRight(start, shift, expected);

    shift = 2; // right
    expected = new int[] {
        0b001010_10,
        0b10_1100_11,
        0b00_0111_01,
    };
    testShiftRight(start, shift, expected);
    

//    0b1010_1010,
//    0b1100_1100,
//    0b0111_0111,
    
    shift = 7;
    expected = new int[] {
        0b0000_000_1,
        0b010_1010_1,
        0b100_1100_0,
    };
    testShiftRight(start, shift, expected);
  }
  
  
  
  
  private void testShiftLeft(byte[] start, int shift, int[] expected) {
    testShiftMany(start, shift, expected, true);
  }
  
  private void testShiftRight(byte[] start, int shift, int[] expected) {
    testShiftMany(start, shift, expected, false);
  }
  
  private void testShiftMany(byte[] start, int shift, int[] expected, boolean left) {
    byte[] array = start.clone();
    
    if (left)
      Bits.shiftLeft(array, shift);
    else
      Bits.shiftRight(array, shift);
    
    byte[] exp = new byte[expected.length];
    for (int index = expected.length; index-- > 0; )
      exp[index] = (byte) expected[index];
    assertArrayEquals(exp, array);
  }
  
  
  
  @Test
  public void testShift61() {
    byte[] start = new byte[61];
    var rand = new Random(11L);
    rand.nextBytes(start);
    
    for (int shift = 0; shift <= 8; ++shift) {
      testLeftAndBack(start, shift);
      testRightAndBack(start, shift);
    }
  }
  
  
  private void testLeftAndBack(byte[] start, int shift) {
    byte[] array = start.clone();
    shiftLeftAndBack(array, shift);
    assertEquals(
        ByteBuffer.wrap(start, 1, start.length - 1),
        ByteBuffer.wrap(array, 1, start.length - 1));
  }
  
  private void testRightAndBack(byte[] start, int shift) {
    byte[] array = start.clone();
    shiftRightAndBack(array, shift);
    assertEquals(
        ByteBuffer.wrap(start, 0, start.length - 1),
        ByteBuffer.wrap(array, 0, start.length - 1));
  }
  
  private void shiftLeftAndBack(byte[] array, int shift) {
    shiftAndBack(array, shift, true);
  }
  
  private void shiftRightAndBack(byte[] array, int shift) {
    shiftAndBack(array, shift, false);
  }
  
  private void shiftAndBack(byte[] array, int shift, boolean left) {
    if (left) {
      Bits.shiftLeft(array, shift);
      Bits.shiftRight(array, shift);
    } else {
      Bits.shiftRight(array, shift);
      Bits.shiftLeft(array, shift);
    }
  }
  
  
  
//  public void testLongBits() {
//    Bits.bits(0b, 0, 0)
//  }

}










