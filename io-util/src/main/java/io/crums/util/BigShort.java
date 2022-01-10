/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * An immutable, unsigned, 3-byte integral value. I need this Goldilocks-sized
 * integral type. Note, this will likely be rewritten as a {@code Primitive}.
 */
public class BigShort extends Number {
  
  
  
  /**
   * Reads and returns 3-bytes as unsigned big endian number.
   * 
   * @return &ge; 0
   */
  public static int getBigShort(ByteBuffer buffer) throws BufferUnderflowException {
    int out = (buffer.get() & 0xff) << 16;
    return out + ((0xffff) & buffer.getShort());
  }
  
  
  /**
   * Puts the given non-negative {@code value} as 3-bytes (big endian) in the
   * give {@code out} buffer.
   * 
   * @throws IllegalArgumentException if {@code value} &lt; 0 or &gt; {@linkplain #MAX_VALUE}
   */
  public static void putBigShort(ByteBuffer out, int value)
      throws BufferOverflowException, IllegalArgumentException {
    
    if (value < 0 || value > MAX_VALUE)
      throw new IllegalArgumentException("value out of range: " + value);
    out.put((byte) (value >> 16));
    out.putShort((short) (value & 0xffff));
  }
  
  
  
  
  /**
   * (If you must serialize.)
   */
  private static final long serialVersionUID = 8734629055865072639L;

  /**
   * The maximum value. 16,777,215.
   */
  public final static int MAX_VALUE = 0xffffff;
  
  /**
   * The number of bytes used in a value's serial representation.
   */
  public final static int BYTES = 3;
  
  
  
  
  
  private final int value;

  
  /**
   * 
   * @param value &ge; 0 and &le; {@linkplain #MAX_VALUE}
   */
  public BigShort(int value) {
    this.value = value;
    if (value < 0)
      throw new IllegalArgumentException("not an unsigned value; " + value);
    if (value > MAX_VALUE)
      throw new IllegalArgumentException(value + " > MAX_VALUE (" + MAX_VALUE + ")");
  }
  
  
  
  public final boolean equals(Object obj) {
    if (obj == this)
      return true;
    else if (obj instanceof BigShort)
      return ((BigShort) obj).value == value;
    else
      return false;
  }
  
  
  public final int hashCode() {
    return value;
  }

  @Override
  public final int intValue() {
    return value;
  }

  @Override
  public final long longValue() {
    return value;
  }

  @Override
  public final float floatValue() {
    return value;
  }

  @Override
  public final double doubleValue() {
    return value;
  }

}
