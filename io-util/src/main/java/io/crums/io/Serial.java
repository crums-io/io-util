/*
 * Copyright 2020-2023 Babak Farhang
 */
package io.crums.io;


import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

/**
 * <p>
 * Simple serialization interface. This serves mostly as documentation, since to
 * be useful an implementation must be complimented with a constructor or
 * pseudo-constructor that <em>loads</em> the binary representation. Typically,
 * this is achieved with something like
 * </p>
 * <pre>
 *   class A implements Serial {
 *     .
 *     .
 *     
 *     static A load(ByteBuffer in) throws BufferUnderflowException, SerialFormatException {
 *        // read from the buffer, advancing its position by
 *        // exactly as many bytes as was written on the way out
 *        try {
 *          .
 *          return new A(..);
 *        } catch (SerialFormatException sfx) {
 *          throw sfx;
 *        } catch (Exception x) {
 *          throw new SerialFormatException("..", x);
 *        }
 *     }
 *   }
 * </pre>
 * 
 * <h2>Self-delimiting</h2>
 * <p>
 * The serialization protocol must be self delimiting. This means one can load
 * an instance from a byte stream or a buffer without reading beyond the last offset
 * of the instance's serial representation.
 * </p>
 * <h2>A Note on Partial Serialization</h2>
 * <p>
 * This is an <em>implementation interface</em> for low level serialization. There
 * is no actual requirement that an instance write its entire state to the buffer:
 * as long as the class is complimented with a static pseudo-constructor that takes
 * both an argument (the left-out state) and a {@code ByteBuffer}, the strategy works.
 * </p>
 * 
 * @see SerialFormatException
 */
public interface Serial {
  
  

  /**
   * Returns the serial size of this instance in bytes.
   */
  int serialSize();
  
  

  /**
   * Writes the {@linkplain #serialize() serial representation} of this instance
   * to the given <code>out</code> buffer. The position of the buffer is advanced by
   * {@linkplain #serialSize()} bytes.
   *          
   * @return <code>out</code> (for invocation chaining)
   * 
   * @throws BufferOverflowException if {@code out} doesn't have adequate remaining
   *          bytes
   * 
   * @see #serialize()
   */
  ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException;
  

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




