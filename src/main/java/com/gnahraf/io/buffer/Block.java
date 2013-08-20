/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.buffer;


import static com.gnahraf.io.buffer.BufferUtils.*;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

/**
 * A contiguous block of cells.
 * 
 * @author Babak
 */
public class Block {
  
  private final static Logger LOG = Logger.getLogger(Block.class);
  
  /**
   * Array of equal capacity cells, windows into the block, in block-offset order.
   * The cell buffers are maintained in clear-ed state.
   */
  protected final ByteBuffer[] cells;
  
  /**
   * The backing buffer. It's capacity is <tt>cells[0].capacity() * cells.length</tt>.
   */
  private final ByteBuffer buffer;
  
  /**
   * The out bound view of the cells. Even read-only <tt>ByteBuffer</tt>s are
   * mutable (you can change their position, for example). So in order to
   * guarantee the boundaries of our cells remain intact, we maintain a
   * separate (but still shared) view of each cell for outbound consumption.
   * <p/>
   * Initialized lazily.
   */
  protected ByteBuffer[] cellsOut;
  
  
  /**
   * Copy constructor available to subclasses; pointless in base class.
   */
  protected Block(Block copy) {
    this.cells = copy.cells;
    this.buffer = copy.buffer;
    this.cellsOut = copy.cellsOut;
  }
  
  
  /**
   * Creates a new instance with the given <tt>block</tt> and <tt>cellByteWidth</tt>.
   * Nags if the block's capacity is not a multiple of the cell width.
   * 
   * @param readOnlyCells
   *        if <tt>true</tt>, then the cells will be read-only. If the <tt>block</tt>
   *        itself is read-only, then this parameter doesn't matter: the cells will
   *        be read-only, regardless.
   */
  public Block(ByteBuffer block, int cellByteWidth, boolean readOnlyCells) {
    
    if (block == null)
      throw new IllegalArgumentException("null block");
    if (cellByteWidth < 1)
      throw new IllegalArgumentException("cellByteWidth: " + cellByteWidth);
    
    final int cellCount = block.capacity() / cellByteWidth;
    
    if (cellCount < 2)
      throw new IllegalArgumentException("cellCount: " + cellCount);
    
    block.clear();
    
    if (block.capacity() % cellByteWidth != 0) {
      LOG.warn(
          "slicing off trailing cell fragment from end of buffer: " +
          "this.block != block (content still shared");
      block.limit(cellCount * cellByteWidth);
      block = block.slice();
    }
    
    this.buffer = block;
    this.cells = new ByteBuffer[cellCount];
    
    ByteBuffer viewBlock =
        readOnlyCells && !this.buffer.isReadOnly() ?
            this.buffer.asReadOnlyBuffer() : this.buffer;
      
    for (int i = 0; i < cellCount; ++i)
      cells[i] = setCell(viewBlock, i, cellByteWidth).slice();
    
    this.buffer.clear();
  }
  
  /**
   * Returns the number of cells in this block.
   */
  public final int cellCount() {
    return cells.length;
  }
  
  /**
   * Returns the number of bytes in each cell.
   */
  public final int cellWidth() {
    return cells[0].capacity();
  }
  
  /**
   * Tests whether this is a read-only instance. If read-only, the block's content cannot
   * be modified through this instance.
   */
  public final boolean isReadOnly() {
    return buffer.isReadOnly();
  }
  
  /**
   * Tests whether this instance's cells are read-only. This method can return <tt>true</tt>
   * even if {@linkplain #isReadOnly()} returns <tt>false</tt>.
   */
  public final boolean readOnlyCells() {
    return cells[0].isReadOnly();
  }
  
  /**
   * Determines whether this instance is made up of direct buffers. Either all
   * or none of the buffers are direct.
   */
  public final boolean isDirect() {
    return buffer.isDirect();
  }
  
  
  /**
   * Returns backing buffer. The capacity of the returned buffer is equals to the sum
   * of the capacities of its cells. Modifications in the returned buffer's content
   * are visible in the {@linkplain #cells() cells} and vice versa. The position and limit of
   * the returned buffer is independent of those of the cells.
   * The returned buffer is read-only, if {@linkplain #isReadOnly()} returns <tt>true</tt>.
   */
  public final ByteBuffer buffer() {
    return buffer;
  }
  
  /**
   * Returns a new copy of the cells array. Each of the cells is a view
   * on to the backing {@linkplain block}. Modifications in any individual element's content
   * are visible in the backing {@linkplain #buffer()} and vice versa. The position and limit of
   * the returned cells (set to zero and capacity, resp.) are independent of any other buffer.
   * The returned buffers are read-only, if {@linkplain #readOnlyCells()} returns <tt>true</tt>.
   * 
   * @see #buffer();
   */
  public final ByteBuffer[] cells() {
    return duplicate(cells);
  }
  
  
  /**
   * Returns a new copy of the cell at the specified <tt>index</tt>. The returned
   * cell is a view on to the backing buffer: modifications in the returned buffer's content
   * are visible in the backing {@linkplain #buffer()} and vice versa. The position and limit of
   * the returned cell (set to zero and capacity, resp.) are independent of any other buffer.
   * The returned buffer is read-only, if {@linkplain #readOnlyCells()} returns <tt>true</tt>.
   * 
   * @throws IndexOutOfBoundsException
   *         if <tt>index</tt> is out of range
   */
  public ByteBuffer cell(int index) {
    return cells[index].duplicate();
  }
  
  

}
