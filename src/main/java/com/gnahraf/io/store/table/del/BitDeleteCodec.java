/*
 * Copyright 2020 Babak Farhang
 */
package com.gnahraf.io.store.table.del;

import java.nio.ByteBuffer;

/**
 * Defines a reserved-bit <tt>DeleteCodec</tt>. Reads and writes only that bit.
 */
public class BitDeleteCodec extends DeleteCodec {
  
  private final int offset;
  private final byte onBit;
  
  
  /**
   * Creates an instance using the hi bit of a byte.
   * 
   * @param offset  non-negative index of the byte examined within a row
   */
  public BitDeleteCodec(int offset) {
    this(offset, Byte.MIN_VALUE);
  }

  
  /**
   * Creates an instance with given bit flag.
   * 
   * @param offset  non-negative index of the byte examined within a row
   * @param onBit   non-zero flag, usually with only one bit turned on
   */
  public BitDeleteCodec(int offset, byte onBit) {
    this.offset = offset;
    this.onBit = onBit;
    
    if (offset < 0)
      throw new IllegalArgumentException("negative offset: " + offset);
    if (onBit == 0)
      throw new IllegalArgumentException("onBit zero");
  }

  
  /**
   * {@inheritDoc}
   * <h3>Implementation</h3>
   * <p>Returns <tt>true</tt> if on reading, the bit flag is on.</p>
   */
  @Override
  public boolean isDeleted(ByteBuffer row) {
    return (row.get(offset) & onBit) == onBit;
  }

  
  /**
   * {@inheritDoc}
   * <h3>Implementation</h3>
   * <p>Writes the bit flag without touching the rest.</p>
   */
  @Override
  public void markDeleted(ByteBuffer row) {
    byte out = row.get(offset);
    out |= onBit;
    row.put(offset, out);
  }

}
