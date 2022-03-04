/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.io;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * Simple serialization interface. This serves mostly as documentation, since to
 * be useful an implementation must be complimented with a constructor or
 * pseudo-constructor that <em>loads</em> the binary representation.
 */
public interface Serial {
  
  

  /**
   * Returns the serial size of this instance in bytes.
   */
  int serialSize();
  
  

  /**
   * Writes the {@linkplain #serialize() serial representation} of this instance
   * to the given <tt>out</tt> buffer. The position of the buffer is advanced by
   * {@linkplain #serialSize()} bytes.
   * 
   * @throws BufferUnderflowException if <tt>out</tt> doesn't have adequate remaining
   *          bytes
   *          
   * @return <tt>out</tt> (for invocation chaining)
   * 
   * @see #serialize()
   */
  ByteBuffer writeTo(ByteBuffer out);
  

  /**
   * Returns a serial (binary) representation of this instance's state.
   * 
   * @see #writeTo(ByteBuffer)
   * @return {@code writeTo(ByteBuffer.allocate(estimateSize()).flip()}
   */
  default ByteBuffer serialize() {
    ByteBuffer out = ByteBuffer.allocate(estimateSize());
    
    writeTo(out);
    
    return out.flip();
  }
  
  
  /**
   * Returns the {@linkplain #serialSize()} or if it's too expensive, a conservative
   * estimate. By default, this returns {@code serialSize()}. Override, if an estimate
   * is significantly cheaper than an exact answer.
   * 
   * @return &ge; {@code serialSize()}
   */
  default int estimateSize() {
    return serialSize();
  }

}




