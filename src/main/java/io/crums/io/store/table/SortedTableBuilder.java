/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table;


import static io.crums.io.store.table.iter.Direction.FORWARD;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.io.buffer.BufferOp;
import io.crums.io.buffer.Covenant;
import io.crums.io.channels.ChannelUtils;
import io.crums.io.store.table.iter.Direction;
import io.crums.io.store.table.iter.RowIterator;
import io.crums.io.store.table.iter.SortedBufferIterator;
import io.crums.io.store.table.order.RowOrder;


/**
 * 
 * @author Babak
 */
public class SortedTableBuilder {

  private final int rowWidth;
  protected final TreeSet<ByteBuffer> sortedView;
  
  
  
  public SortedTableBuilder(int rowWidth, RowOrder order) {
    if (rowWidth < 1)
      throw new IllegalArgumentException("rowWidth: " + rowWidth);
    if (order == null)
      throw new IllegalArgumentException("null row order");
    
    this.rowWidth = rowWidth;
    this.sortedView = new TreeSet<>(order);
  }
  
  
  
  public boolean putRow(ByteBuffer row) throws IOException {
    return putRow(row, Covenant.NONE);
  }
  
  
  public boolean putRow(ByteBuffer row, Covenant promise) throws IOException {
    if (row == null)
      throw new IllegalArgumentException("null row");
    if (row.remaining() != rowWidth)
      throw new IllegalArgumentException(
          "row does not contain required remaining bytes (rowWidth=" + rowWidth +
          "): " + row);
    
    if (promise == null)
      promise = Covenant.NONE;
    
    ByteBuffer tableRow;
    if (promise.wontModify()) {
      if (row.position() == 0)
        tableRow = row;
      else 
        tableRow = row.slice();
    } else if (promise.wontModifyContents()) {
      tableRow = row.slice();
    } else {
      tableRow = allocateRow();
      tableRow.clear();
      tableRow.put(row).flip();
    }
    
    

    preInsertion(tableRow);
    boolean overwrite = overwrite(tableRow);
    postInsertion(tableRow);
    return overwrite;
  }
  
  
  public RowIterator iterator(ByteBuffer key, Direction direction, boolean includeKey) {
    if (direction == null)
      throw new IllegalArgumentException("null direction");
    if (key == null || !key.hasRemaining())
      throw new IllegalArgumentException("key: " + key);
    
    NavigableSet<ByteBuffer> subset;
    if (direction == FORWARD)
      subset = sortedView.tailSet(key, includeKey);
    else
      subset = sortedView.headSet(key, includeKey);
    return new SortedBufferIterator(subset, direction, rowWidth);
  }
  
  
  protected final boolean overwrite(ByteBuffer tableRow) {
    boolean overwrite = !sortedView.add(tableRow);
    if (overwrite) {
      if ( !sortedView.remove(tableRow) )
        throw new RuntimeException("assertion failure on remove. Bad set implementation?");
      if ( !sortedView.add(tableRow) )
        throw new RuntimeException("assertion failure on add. Bad set implementation?");
    }
    return overwrite;
  }
  
  
  /**
   * Pre insertion hook. <em>Remember, on return, the argument shouldn't be modified
   * in <strong>any</strong> way!</em>
   */
  protected void preInsertion(ByteBuffer rows) throws IOException {
    
  }
  
  /**
   * Post insertion hook. <em>Remember, on return, the argument shouldn't be modified
   * in <strong>any</strong> way!</em>
   */
  protected void postInsertion(ByteBuffer rows) throws IOException {
    
  }



  public int putRows(ByteBuffer rows, Covenant promise) throws IOException {
    if (rows == null)
      throw new IllegalArgumentException("null rows");
    int count = rows.remaining() / rowWidth;
    if (count == 0 || count * rowWidth != rows.remaining())
      throw new IllegalArgumentException(
          "rows must contain a nonzero mulitiple of rowWidth (" + rowWidth +
          ") remaining bytes: " + rows);
    if (promise == null)
      promise = Covenant.NONE;
    
    final int initPos = rows.position();
    int pos = initPos;
    
    final int limit = rows.limit();
    // if the caller says they'll reuse the buffer
    if (!promise.wontModifyContents()) {
      rows = ByteBuffer.allocate(limit - pos).put(rows);
      rows.flip();
    }
    preInsertion(rows);
    int overwrites = 0;
    while (pos < limit) {
      int runlimit = pos + rowWidth;
      rows.limit(runlimit);
      ByteBuffer row = rows.slice();
      
      if (overwrite(row))
        ++overwrites;
      pos = runlimit;
      rows.position(pos);
    }
    rows.position(initPos);
    postInsertion(rows);
    
    return overwrites;
  }
  
  
  public int putRows(ByteBuffer[] rows) throws IOException {
    // TODO
    return 0;
  }
  
  
  
  public boolean isEmpty() {
    return sortedView.isEmpty();
  }
  
  
  public int getRowCount() {
    return sortedView.size();
  }
  
  
  public final int getRowWidth() {
    return rowWidth;
  }
  
  public void clear() {
    sortedView.clear();
  }
  
  
  public ByteBuffer getRow(ByteBuffer rowKey) {
    ByteBuffer out = getImpl(rowKey);
    if (out == null)
      return null;
    else
      return out.asReadOnlyBuffer();
  }
  
  
  private ByteBuffer getImpl(ByteBuffer rowKey) {
    SortedSet<ByteBuffer> tailSet = sortedView.tailSet(rowKey);
    if (tailSet.isEmpty())
      return null;
    ByteBuffer out = tailSet.first();
    if (sortedView.comparator().compare(rowKey, out) == 0)
      return out;
    else
      return null;
  }
  
  
  public boolean readRow(ByteBuffer rowKey, ByteBuffer out) {
    if (out == null || out.remaining() < rowWidth)
      throw new IllegalArgumentException("out buffer underflow: " + out);
    
    ByteBuffer match = getImpl(rowKey);
    if (match == null)
      return false;
    
    match.mark();
    // Observation: if we fail below, our data structure may get corrupted. If.
    // Because the position of the matched buffer is modified below,
    // if the row order is relative, then the sortedView's state may be f%#ked
    // if accessed while this operation is in progress--particularly if
    // its later written to. If the row order is absolute, however, since the
    // buffer's limit is not modified there's no problem.
    // When could this happen?
    // If another thread concurrently modified any offset of _out_
    // (mark, clear, reset, position, limit, flip) in a way that caused
    // it to overflow.
    out.put(match);
    match.reset();
    return true;
  }
  
  
  /**
   * Returns the number of bytes the {@linkplain #flush(GatheringByteChannel, boolean) flush}ed
   * table will take.
   */
  public long byteSize() {
    return sortedView.size() * (long) rowWidth;
  }
  
  
  /**
   * Flushes the sorted contents to the given <tt>file</tt>.
   * 
   * @param file
   *        typically a {@linkplain FileChannel}
   * @param clear
   *        if <tt>true</tt>, then the {@linkplain #clear()} is called on return.
   *        The reason why the parameter is offered is it's marginally more efficient
   *        to pass <tt>true</tt> than to pass <tt>false</tt> and followed by <tt>clear()</tt>.
   */
  public void flush(GatheringByteChannel file, boolean clear) throws IOException {
    if (file == null)
      throw new IllegalArgumentException("null file");
    if (isEmpty())
      return;
    ByteBuffer[] rowBufs = sortedView.toArray(new ByteBuffer[sortedView.size()]);
    if (!clear)
      BufferOp.MARK.opAll(rowBufs);
    ChannelUtils.writeRemaining(file, rowBufs);
    if (clear)
      sortedView.clear();
    else
      BufferOp.RESET.opAll(rowBufs);
  }
  
  
  private ByteBuffer allocateRow() {
    return allocateRows(1);
  }
  
  protected ByteBuffer allocateRows(int count) {
    return ByteBuffer.allocate(rowWidth * count);
  }
  
}
