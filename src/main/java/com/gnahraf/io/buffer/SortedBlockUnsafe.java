/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.buffer;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * Offers a faster, but unsafe view into a sorted block's rows. A sorted
 * block is often an internal field of another class. In such cases, as long as
 * the implementation is careful, there's really nothing unsafe about its use.
 * 
 * @see #cellsXprt()
 * @author Babak
 */
public class SortedBlockUnsafe extends SortedBlock {

  public SortedBlockUnsafe(SortedBlock copy) {
    super(copy, copy.order());
  }


  public SortedBlockUnsafe(
      ByteBuffer block, int cellByteWidth, Comparator<ByteBuffer> order) {
    super(block, cellByteWidth, order);
  }


  public SortedBlockUnsafe(
      ByteBuffer block, int cellByteWidth, Comparator<ByteBuffer> order,
      boolean readOnlyCells) {
    
    super(block, cellByteWidth, order, readOnlyCells);
  }
  
  
  /**
   * Expert. Returns the cell view buffers of the block.
   * <p/>
   * <h2>Warning</h2>
   * <em>Treat this as a read-only object!</em> Do not modify the returned array
   * or the offsets (position and limit) of the <tt>ByteBuffer</tt>s in the
   * array. Remember, modifying the size of a buffer too influences its order
   * relative to other buffers (rows). If you really have to, though (contrived
   * example: dump buffer contents to a stream or socket), then
   * be sure to clear them when you're done (as in {@linkplain
   * BufferUtils#clearAll(ByteBuffer[])}). <em>Unless you wont be using this
   * instance any more, don't expose any part of the returned
   * array to the outside world!</em>
   * <p/>
   * The advantage this unsafe method offers is efficiency. The {@linkplain Block#cells()
   * cells()} method incurs a significant overhead on first invocation, and then
   * continues to churn on subsequent calls, all in order to guarantee an instance's
   * state cannot be broken.
   */
  public ByteBuffer[] cellsXprt() {
    return cells;
  }
  
  

}
