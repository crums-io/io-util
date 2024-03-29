/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.buffer;


import java.nio.ByteBuffer;

/**
 * A unary buffer operation.
 */
public abstract class BufferOp {
  
  private static abstract class InplaceOp extends BufferOp {
    @Override
    public ByteBuffer[] opAll(ByteBuffer[] buffers) {
      return defaultOpAllImpl(buffers, buffers);
    }
  }
  
  /**
   * Duplicates the buffer. The input buffer is unmodified.
   * {@linkplain #opAll(ByteBuffer[])} returns a new array.
   */
  public final static BufferOp DUPLICATE =
      new BufferOp() {
        @Override
        public ByteBuffer op(ByteBuffer buffer) {
          return buffer.duplicate();
        }
      };

      
  /**
   * Duplicates the buffer. The input buffer is unmodified.
   * {@linkplain #opAll(ByteBuffer[])} returns the input array.
   */
  public final static BufferOp DUPLICATE_INPLACE =
      new InplaceOp() {
        @Override
        public ByteBuffer op(ByteBuffer buffer) {
          return buffer.duplicate();
        }
      };
      
      
  /**
   * Slices the buffer. The input buffer is unmodified.
   * {@linkplain #opAll(ByteBuffer[])} returns a new array.
   */
  public final static BufferOp SLICE =
      new BufferOp() {
        @Override
        public ByteBuffer op(ByteBuffer buffer) {
          return buffer.slice();
        }
      };

      
  /**
   * Slices the buffer. The input buffer is unmodified.
   * {@linkplain #opAll(ByteBuffer[])} returns the input array.
   */
  public final static BufferOp SLICE_INPLACE =
      new InplaceOp() {
        @Override
        public ByteBuffer op(ByteBuffer buffer) {
          return buffer.slice();
        }
      };


  /**
   * Clears the buffer. The input buffer is <em>modified</em>.
   * {@linkplain #opAll(ByteBuffer[])} returns the input array.
   * 
   */
  public final static BufferOp CLEAR =
      new InplaceOp() {
        @Override
        public ByteBuffer op(ByteBuffer buffer) {
          buffer.clear();
          return buffer;
        }
      };

  /**
   * Marks the buffer. The input buffer is <em>modified</em>.
   * {@linkplain #opAll(ByteBuffer[])} returns the input array.
   * 
   */
  public final static BufferOp MARK =
      new InplaceOp() {
        @Override
        public ByteBuffer op(ByteBuffer buffer) {
          buffer.mark();
          return buffer;
        }
      };

  /**
   * Resets the buffer. The input buffer is <em>modified</em>.
   * {@linkplain #opAll(ByteBuffer[])} returns the input array.
   * 
   */
  public final static BufferOp RESET =
      new InplaceOp() {
        @Override
        public ByteBuffer op(ByteBuffer buffer) {
          buffer.reset();
          return buffer;
        }
      };
  

  /**
   * Creates a new read-only view of the buffer. The input buffer is unmodified.
   * {@linkplain #opAll(ByteBuffer[])} returns a new array.
   */
  public final static BufferOp AS_READONLY =
      new BufferOp() {
        @Override
        public ByteBuffer op(ByteBuffer buffer) {
          return buffer.asReadOnlyBuffer();
        }
      };
  

  /**
   * Creates a new read-only view of the buffer. The input buffer is unmodified.
   * {@linkplain #opAll(ByteBuffer[])} returns the input array.
   */
  public final static BufferOp AS_READONLY_INPLACE =
      new InplaceOp() {
        @Override
        public ByteBuffer op(ByteBuffer buffer) {
          return buffer.asReadOnlyBuffer();
        }
      };
      



  /**
   * <p>
   * Ensures the input buffer is read-only; if read-only, the input is returned;
   * otherwise, a read-only view of the input is created and returned.
   * The input buffer itself is unmodified.
   * </p><p>
   * {@linkplain #opAll(ByteBuffer[])} returns the input array, replacing
   * any writable elements with read-only views of the same.
   * </p>
   */
  public final static BufferOp ENSURE_READONLY =
      new InplaceOp() {
        @Override
        public ByteBuffer op(ByteBuffer buffer) {
          return buffer.isReadOnly() ? buffer : buffer.asReadOnlyBuffer();
        }
      };
 
 
  
 
 
 
 
 
 /**
  * Performs an operation on the given buffer, returning the result.
  * The result may be the same instance (modified), or another instance.
  * 
  * @param buffer
  * @return
  */
 public abstract ByteBuffer op(ByteBuffer buffer);
 
 
 public ByteBuffer[] opAll(ByteBuffer[] buffers) {
   return defaultOpAllImpl(buffers, new ByteBuffer[buffers.length]);
 }
 
 protected final ByteBuffer[] defaultOpAllImpl(ByteBuffer[] inputs, ByteBuffer[] results) {
   for (int i = inputs.length; i-- > 0; )
     results[i] = op(inputs[i]);
   return results;
 }

}
