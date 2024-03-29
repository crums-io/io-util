/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.buffer;


import static io.crums.io.buffer.BufferUtils.*;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

/**
 * A contiguous block of cells.
 */
public class Block {
  
  /**
   * Array of equal capacity cells, windows into the block, in block-offset order.
   * The cell buffers are maintained in clear-ed state.
   */
  protected final ByteBuffer[] cells;
  
  /**
   * The backing buffer. It's capacity is <code>cells[0].capacity() * cells.length</code>.
   */
  private final ByteBuffer buffer;
  
  
  /**
   * Copy constructor. The new instance shares the same backing buffers. That is, content
   * modifications in one buffer are also visible in the other.
   */
  public Block(Block copy) {
    this.cells = copy.cells.clone();
    this.buffer = copy.buffer.duplicate();
  }
  
  
  /**
   * Creates a new instance with the given <code>block</code> and <code>cellByteWidth</code>.
   * Nags if the block's capacity is not a multiple of the cell width.
   * 
   * @param readOnlyCells
   *        if <code>true</code>, then the cells will be read-only. If the <code>block</code>
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
   * Tests whether this instance's cells are read-only. This method can return <code>true</code>
   * even if {@linkplain #isReadOnly()} returns <code>false</code>.
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
   * Returns the backing buffer. The capacity of the returned buffer is equals to the sum
   * of the capacities of its cells. Modifications in the returned buffer's content
   * are visible in the {@linkplain #cells() cells} and vice versa. The position and limit of
   * the returned buffer is independent of those of the cells.
   * The returned buffer is read-only, if {@linkplain #isReadOnly()} returns <code>true</code>.
   * Modifications to the buffer's position and limit are ignored by this class.
   * The contents of the returned buffer may be ordered differently than the order of
   * this instance's cells.
   */
  public final ByteBuffer buffer() {
    return buffer.duplicate();
  }
  
  /**
   * Returns a new copy of the cells array. Each of the cells is a view
   * on to the backing {@linkplain Block#buffer()}. Modifications in any individual element's content
   * are visible in the backing {@linkplain #buffer()} and vice versa. The position and limit of
   * the returned cells (set to zero and capacity, resp.) are independent of any other buffer.
   * The returned buffers are read-only, if {@linkplain #readOnlyCells()} returns <code>true</code>.
   * 
   * @see #buffer()
   */
  public final ByteBuffer[] cells() {
    return duplicate(cells);
  }
  
  
  /**
   * Returns a new view of the cell at the specified <code>index</code>. The returned
   * cell is a view on to the backing buffer: modifications in the returned buffer's content
   * are visible in the backing {@linkplain #buffer()} and vice versa. The position and limit of
   * the returned cell (set to zero and capacity, resp.) are independent of any other buffer.
   * The returned buffer is read-only, if {@linkplain #readOnlyCells()} returns <code>true</code>.
   * 
   * @throws IndexOutOfBoundsException
   *         if <code>index</code> is out of range
   */
  public ByteBuffer cell(int index) {
    return cells[index].duplicate();
  }
  
  
  /**
   * Copies the contents of the cell at the given <code>index</code> into the
   * <code>buffer</code>. The position of the given buffer is advanced by the
   * {@linkplain #cellWidth() cell width}.
   * 
   * @throws BufferOverflowException
   *             
   */
  public void copyCellInto(int index, ByteBuffer buffer) throws BufferOverflowException {
    ByteBuffer cell = cell(index);
    if (cell.remaining() != cell.capacity())
      throw new IllegalStateException(
          "Assertion failure at cell[" + index + "]=" + cell + " Illegal concurrent access?");
    buffer.put(cell);
  }
  
}
