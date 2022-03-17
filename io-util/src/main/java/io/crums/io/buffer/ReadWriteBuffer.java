/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.io.buffer;


import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Read-write idiom using <tt>ByteBuffer</tt>s. Similar to that provided in Netty, this
 * partitions a <tt>ByteBuffer</tt> into readable and writable parts.
 */
public class ReadWriteBuffer {
  
  
  private final ByteBuffer writeBuffer;

  
  /**
   * Creates a new instance with its own backing buffer (not direct).
   * 
   * @param capacity the capacity of the buffer.
   */
  public ReadWriteBuffer(int capacity) {
    this(capacity, false);
  }

  /**
   * Creates a new instance with its own backing buffer.
   * 
   * @param capacity the capacity of the buffer.
   * 
   * @param direct <tt>true</tt> if a direct buffer (default <tt>false</tt>)
   */
  public ReadWriteBuffer(int capacity, boolean direct) {
    this(direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity), null);
  }
  
  
  /**
   * Creates a new instance with the given buffer. Positional state is respected,
   * however the instance works with a duplicate view of the given buffer so that this
   * class can provide its positional guarantees.
   * 
   * @param writeBuffer (not read-only)
   */
  public ReadWriteBuffer(ByteBuffer writeBuffer) {
    this(writeBuffer.duplicate(), null);
  }
  
  
  private ReadWriteBuffer(ByteBuffer writeBuffer, Object ignored) {
    this.writeBuffer = Objects.requireNonNull(writeBuffer);
    if (writeBuffer.isReadOnly())
      throw new IllegalArgumentException("writeBuffer " + writeBuffer + " is read-only");
  }
  
  
  /**
   * Puts the given data. On return the {@linkplain #readBuffer() readBuffer}'s limit is advanced.
   * 
   * @return <tt>this</tt>
   */
  public final ReadWriteBuffer put(ByteBuffer data) {
    writeBuffer.put(data);
    return this;
  }
  
  
  /**
   * Sets the position. Equivalently, this (re-)sets the number of bytes written.
   * 
   * @param position &ge; 0 and &le; {@linkplain #capacity()}
   * @see #position(int)
   */
  public final ReadWriteBuffer position(int position) {
    writeBuffer.position(position);
    return this;
  }
  
  
  /**
   * Returns the index where the next write would occur. Equivalently, this just a
   * synonym for {@linkplain #readableBytes()}.
   */
  public final int position() {
    return writeBuffer.position();
  }
  
  
  /**
   * Returns the readBuffer.
   * 
   * @return read-only sliced view
   */
  public final ByteBuffer readBuffer() {
    return writeBuffer.asReadOnlyBuffer().flip().slice();
  }
  
  public ByteBuffer copyReadBuffer() {
    int size = writeBuffer.position();
    return
        size == 0 ?
            BufferUtils.NULL_BUFFER :
              copyReadBufferInto(ByteBuffer.allocate(size)).flip();
    
  }
  
  public final ByteBuffer copyReadBufferInto(ByteBuffer out) {
    if (out.remaining() < readableBytes())
      throw new IllegalArgumentException("required " + readableBytes() + " remaining: " + out);
    
    return out.put(writeBuffer.duplicate().flip());
  }
  
  
  /**
   * Returns the number of bytes that can be read. (Equivalently, this is the number
   * of bytes thus far written.)
   */
  public final int readableBytes() {
    return writeBuffer.position();
  }
  
  
  /**
   * Returns the number of bytes that can be written before breaching capacity.
   * @return
   */
  public final int writeableBytes() {
    return writeBuffer.remaining();
  }
  
  
  /**
   * Returns the capacity in bytes.
   */
  public final int capacity() {
    return writeBuffer.capacity();
  }
  
  
  /**
   * Clears the state for a new round of writes. On return, the {@linkplain #readBuffer() readBuffer}
   * is empty.
   * 
   * @return <tt>this</tt>
   */
  public final ReadWriteBuffer clear() {
    writeBuffer.clear();
    return this;
  }

}






