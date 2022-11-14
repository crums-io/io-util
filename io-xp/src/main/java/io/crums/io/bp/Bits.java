/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.io.bp;

/**
 * Utilities for shifting and patching bits across bytes.
 */
public class Bits {
  // never
  private Bits() {  }
  
  
  /**
   * Patches together and returns an 8-bit value using the 2 given bytes,
   * starting from the given bit offset. The 2 given bytes {@code a}
   * and {@code b} are modeled to define bits 0 thru 15, with hi bits
   * of bytes {@code a} and {@code b} indexed at 0 abd 8, respectively.
   * 
   * @param a       the first byte
   * @param b       the second byte
   * @param offset  starting bit offset (0 &le; {@code offset} &le; 8)
   * 
   * @return 8-bits concatenated from the starting bit {@code offset}
   */
  public static byte patchBits(byte a, byte b, int offset) {
    checkBits(offset, "offset");
    return patch(a, b, offset);
  }
  
  
  private static byte patch(byte a, byte b, int offset) {
    int ua = a & 0xff;
    int ub = b & 0xff;
    return (byte) ((ua << offset) | ub >>> (8 - offset));
  }
  
  private static void checkBits(int offset, String name) {
    if (offset < 0 || offset > 8)
      throw new IllegalArgumentException(
          String.format("out-of-bounds %s: %d", name, offset));
  }
  
  
  /**
   * Shift the given byte array to the left by the given number of bits.
   * Rightside holes are zeroed.
   * 
   * @param array   not null
   * @param bits    the shift amount (0 &le; {@code bits} &le; 8)
   */
  public static void shiftLeft(byte[] array, int bits) {
    checkBits(bits, "bits");
    
    switch (array.length) {
    case 1: array[0] <<= bits;
    case 0: return;
    default:
    }
    
    // array.length >= 2
    
    switch (bits) {
    case 8:
      for (int index = 1; index < array.length; ++index)
        array[index - 1] = array[index];
      array[array.length - 1] = 0;
    case 0:
      return;
    default:
    }
    
    // bits range: [1,7]
    final int lastIndex = array.length - 1; // >= 0
    
    for (int index = 0; index < lastIndex; ++index)
      array[index] = patch(array[index], array[index + 1], bits);
    array[lastIndex] <<= bits;
  }
  
  
  /**
   * Shift the given byte array to the right by the given number of bits.
   * Leftside holes are zeroed.
   * 
   * @param array   not null
   * @param bits    the shift amount (0 &le; {@code bits} &le; 8)
   */
  public static void shiftRight(byte[] array, int bits) {
    checkBits(bits, "bits");
    
    switch (array.length) {
    case 1: array[0] = rightShift(array[0], bits);
    case 0: return;
    default:
    }
    
    // array.length >= 2
    
    switch (bits) {
    case 8:
      for (int index = array.length; index-- > 1;)
        array[index] = array[index - 1];
      array[0] = 0;
    case 0:
      return;
    default:
    }

    // bits range: [1,7] => offset range same
    final int offset = 8 - bits;
    for (int index = array.length; index-- > 1; )
      array[index] = patch(array[index - 1], array[index], offset);
    array[0] = rightShift(array[0], bits);
  }
  
  
  /**
   * Returns the unsigned right-shift of the given byte.
   * Easy to get wrong in Java.
   * 
   * @param b     the byte
   * @param bits  the amount shifted to the right
   * 
   * @return {@code (byte) ((b & 0xff) >>> bits)}
   */
  public static byte rightShift(byte b, int bits) {
    return (byte) ((b & 0xff) >>> bits);
  }
  
  
  
  
  /**
   * Extracts the specified bits from the given 8-byte {@code value} at the
   * given bit offsets. Note the offsets are left-to-right, starting from zero,
   * with 0 being the hi-bit, and 63, the lo-bit (least significant). This is
   * opposite convention, but makes sense from this class's perspective.
   * 
   * @param value     64-bit value
   * @param startBit  start bit index (0 is the highest bit)
   * @param endBit    end bit index (exclusive)
   * 
   * @return byte containing the bits, packed to the right
   */
  public static byte bits(long value, int startBit, int endBit) {
    if (startBit < 0 || startBit > 63)
      throw new IllegalArgumentException("out-of-bounds startBit: " + startBit);
    
    final int bits = endBit - startBit;
    if (bits > 8 || bits < 1)
      throw new IllegalArgumentException("startBit, endBit: " + startBit + ", " + endBit);
    
    value >>>= (64 - endBit);
    value &= (1L << bits) - 1;
    return (byte) value;
  }
  
  
}
















