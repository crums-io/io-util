/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util;


import java.nio.ByteBuffer;
import java.security.SecureRandom;

/**
 * An immutable 16-byte random ID. Our main use is an ephemeral, per-process identifier.
 * 
 * @see #RUN_INSTANCE
 */
public class RandomId {
  
  /**
   * Each time a {@linkplain ClassLoader} loads this class, this takes on a new, likely never
   * seen before value.
   */
  public final static RandomId RUN_INSTANCE = new RandomId();

  public final static int LENGTH = 16;
  
  private final ByteBuffer id;
  
  private final String hexId;

  /**
   * 
   */
  public RandomId() {
    SecureRandom random = new SecureRandom();
    byte[] array = new byte[LENGTH];
    random.nextBytes(array);
    this.id = ByteBuffer.wrap(array).asReadOnlyBuffer();
    this.hexId = IntegralStrings.toHex(id);
  }
  
  /**
   * Returns the (likely) globally unique, run-specific ID.
   */
  public final ByteBuffer id() {
    return id.asReadOnlyBuffer();
  }
  
  /**
   * Returns the {@linkplain #id() ID} as a hexadecimal string.
   */
  public final String hexId() {
    return hexId;
  }
  
  public final long seed() {
    return id.getLong(3);
  }
  
  /**
   * Two instances are equal if they have the same {@linkplain #id() ID}.
   */
  @Override
  public final boolean equals(Object o) {
    return o == this || (o instanceof RandomId) && id.equals(((RandomId) o).id);
  }
  
  /**
   * Consistent with {@linkplain #equals(Object)}.
   */
  @Override
  public final int hashCode() {
    return id.getInt(0);
  }
  
  
  /**
   * Returns the {@linkplain #hexId()}.
   */
  public final String toString() {
    // NOTE: code uses this logic. Don't change it.
    return hexId;
  }

}
