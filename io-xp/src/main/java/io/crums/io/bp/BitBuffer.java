/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.io.bp;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

/**
 * A buffer of bits. This is modeled like a {@linkplain ByteBuffer}, in that it keeps
 * track of the buffer's <em>bit</em> position, as more bits are appended to it.
 * 
 * <h2>Input</h2>
 * <p>
 * Input bits are expressed in whole or fractional bytes:
 * </p>
 * <ul>
 * <li>{@linkplain #put(byte)}. Appends 8-bits.</li>
 * <li>{@linkplain #putLeft(byte, int)}. Appends the specified hi-bits of the given byte.</li>
 * <li>{@linkplain #putRight(byte, int)}. Appends the specified lo-bits of the given byte.</li>
 * </ul>
 */
public class BitBuffer {
  
  public final static int MIN_CAPACITY = 8;
  public final static int MAX_CAPACITY = Integer.MAX_VALUE / 8;
  
  private final byte[] barray;
  private final ByteBuffer buffer;
  private final int maxBits;
  
  private int bits;

  /**
   * Creates a new instance with given byte capacity.
   * 
   * @param capacity &ge; 8 and &le; {@linkplain #MAX_CAPACITY}
   */
  public BitBuffer(int capacity) {
    checkCapacity(capacity);
    
    this.barray = new byte[capacity];
    this.buffer = ByteBuffer.wrap(barray);
    this.maxBits = capacity * 8;
  }
  
  
  /**
   * Creates a new instance with the given backing array (not copied).
   * 
   * @param array   of length &ge; 8 and &le; {@linkplain #MAX_CAPACITY}
   */
  public BitBuffer(byte[] array) {
    checkCapacity(array.length);
    
    this.barray = array;
    this.buffer = ByteBuffer.wrap(barray);
    this.maxBits = array.length * 8;
  }
  
  
  private void checkCapacity(int capacity) {
    if (capacity < MIN_CAPACITY)
      throw new IllegalArgumentException(
          "capacity " + capacity + " < " + MIN_CAPACITY);
    if (capacity > MAX_CAPACITY)
      throw new IllegalArgumentException(
          "capacity " + capacity + " > 0x" + Integer.toHexString(MAX_CAPACITY));
  }
  
  
  
  
  
  /**
   * Puts the leftmost (hi) bits in the given byte. Advances the
   * {@linkplain #bitPosition()} by {@code lbits}.
   * 
   * @param b       byte containing the bits
   * @param lbits   the number of leftmost bits (1 &le; {@code lbits} &le; 8)
   * @return {@code this}
   */
  public BitBuffer putLeft(byte b, int lbits) {
    checkBits(lbits);
    putLeftUnchecked(b, lbits);
    return this;
  }
  
  /**
   * Puts the rightmost (lo) bits in the given byte. Advances the
   * {@linkplain #bitPosition()} by {@code lbits}.
   * 
   * @param b       byte containing the bits
   * @param rbits   the number of rightmost bits (1 &le; {@code rbits} &le; 8)
   * @return {@code this}
   */
  public BitBuffer putRight(byte b, int rbits) {
    checkBits(rbits);
    int unsigned = b & 0xff;
    putLeftUnchecked((byte) (unsigned << (8 - rbits)), rbits);
    return this;
  }
  
  /** Puts the given 8-bits. */
  public BitBuffer put(byte b) {
    putLeftUnchecked(b, 8);
    return this;
  }
  
  
  /**
   * Clears the state of the buffer.
   * @return {@code this}
   */
  public BitBuffer clear() {
    buffer.clear();
    bits = 0;
    return this;
  }
  
  
  
  public BitBuffer clearFullBytes() {
    final int lastBits = bits & 7;
    if (lastBits == 0)
      return clear();
    byte b = barray[buffer.position() - 1];
    clear().putLeftUnchecked(b, lastBits);
    return this;
  }
  
  
  /**
   * Returns the a read-only view of the current state as a {@code ByteBuffer}.
   * If the last byte is fractional (i.e. if the bit-position is not a multiple of 8),
   * then the trailing bits of the last byte are unspecified (you typically don't
   * care about them).
   */
  public ByteBuffer asByteBuffer() {
    return buffer.asReadOnlyBuffer().flip();
  }
  
  /** Returns the byte-length. */
  public int byteLength() {
    return buffer.position();
  }
  
  /** Returns the bit position. Equivalently, the number of bits put. */
  public int bitPosition() {
    return bits;
  }
  
  /** Sets the bit position. */
  public BitBuffer bitPosition(int bits) {
    this.bits = bits;
    buffer.position((bits + 7) / 8);
    return this;
  }
  
  
  private void putLeftUnchecked(byte b, int lbits) {
    final int lastBits = bits & 7;
    if (lastBits == 0) {
      // we don't erase the trailing bits
      buffer.put(b);
      
    } else {
      
      final int bIndex = buffer.position() - 1;
      final int unsignedB = b & 0xff;
      final int bitsConsumed = 8 - lastBits;
      final int ubLeft, ubRight;
      {
        int mask = 0xff - ((1 << bitsConsumed) - 1);
        ubLeft = barray[bIndex] & mask;
      }
      
      ubRight = unsignedB >>> lastBits;
      
      int b0 = ubLeft | ubRight;
      assert b0 == ubLeft + ubRight;
      barray[bIndex] = (byte) b0;
      
      if (bitsConsumed < lbits) {
        int wb = unsignedB << bitsConsumed;  // check: doubt &'ing necessary
        buffer.put((byte) wb);
      }
    }
    
    bits += lbits;
  }
  
  private void checkBits(int lbits) {
    if (lbits < 1 || lbits > 8)
      throw new IllegalArgumentException("out-of-bounds bits: " + lbits);
    if (bits + lbits > maxBits)
      throw new BufferOverflowException();
  }
  

}
















