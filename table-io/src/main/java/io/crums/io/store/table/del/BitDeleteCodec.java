/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.io.store.table.del;

import java.nio.ByteBuffer;

/**
 * Defines a reserved-bit <code>DeleteCodec</code>. Reads and writes only that bit.
 * <p>
 * Actually, this works with any number of on-bits in the byte, but it was
 * designed for a single bit. (Why would you need more than 1 bit? No idea..
 * Maybe if each bit represented a milestone in a lifecycle and you didn't want
 * to delete until all they're all checked off (?).
 * </p>
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
   * <h4>Implementation</h4>
   * <p>Returns <code>true</code> if on reading, the bit flag is on.</p>
   */
  @Override
  public boolean isDeleted(ByteBuffer row) {
    return (row.get(offset) & onBit) == onBit;
  }

  
  /**
   * {@inheritDoc}
   * <h4>Implementation</h4>
   * <p>Writes the bit flag without touching the rest.</p>
   */
  @Override
  public void markDeleted(ByteBuffer row) {
    byte out = row.get(offset);
    out |= onBit;
    row.put(offset, out);
  }
  
  
  @Override
  public final boolean equals(Object o) {
    if (o == this)
      return true;
    if (o instanceof BitDeleteCodec) {
      BitDeleteCodec other = (BitDeleteCodec) o;
      return offset == other.offset && onBit == other.onBit;
    }
    return false;
  }
  
  
  @Override
  public final int hashCode() {
    return offset ^ (int) onBit;
  }
  
  
  @Override
  public String toString() {
    return BitDeleteCodec.class.getSimpleName() +
        "[" + offset + ":" +
        Integer.toBinaryString(Byte.toUnsignedInt(onBit)) + "]";
  }

}
