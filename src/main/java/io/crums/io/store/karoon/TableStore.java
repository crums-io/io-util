/*
 * Copyright 2020 Babak Farhang 
 */
package io.crums.io.store.karoon;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;

import io.crums.io.buffer.Covenant;
import io.crums.io.store.Sorted;
import io.crums.io.store.table.del.DeleteCodec;
import io.crums.io.store.table.iter.Direction;
import io.crums.io.store.table.order.RowOrder;

/**
 * A logical table sorted table. Interface extracted from {@linkplain TStore} (which implements it
 * as a stack of on disk tables).
 */
public interface TableStore extends Channel, Sorted {
  
  
  /**
   * Returns the table name.
   */
  String name();
  
  
  /**
   * Returns the row width in bytes.
   */
  int rowWidth();
  
  
  /**
   * Returns the row order.
   */
  RowOrder rowOrder();
  
  
  /**
   * Returns the optional deleteCodec. If this method returns <tt>null</tt>,
   * then deletes are not supported.
   */
  DeleteCodec deleteCodec();
  
  
  /**
   * Returns the row with the given matching <tt>key</tt>; or <tt>null</tt>, if not found.
   */
  ByteBuffer getRow(ByteBuffer key) throws IOException;
  

  /**
   * Searches and returns the next row starting from the given <tt>key</tt>.
   * 
   * @return the next row, or <tt>null</tt> if no such row exists
   */
  ByteBuffer nextRow(ByteBuffer key, Direction direction, boolean includeKey) throws IOException;

  /**
   * Inserts or updates the given <tt>row</tt> with no promise/covenant. Shorthand for
   * {@linkplain #setRow(ByteBuffer, Covenant) setRow(row, Covenant.NONE)}
   * 
   * @param row
   *        the remaining bytes in this buffer represent the row being input. The
   *        remaining bytes must be exactly equal to {@linkplain #rowWidth()}.
   */
  default void setRow(ByteBuffer row) throws IOException {
    setRow(row, Covenant.NONE);
  }

  /**
   * Inserts or updates the given <tt>row</tt>. The operation is fail-safe (all-or-nothing).
   * <p/>
   * Pay attention to the <tt>promise</tt> parameter. It has a huge impact on performance,
   * but just as importantly, if you break the promise, you break the data set.
   * 
   * @param row
   *        the remaining bytes in this buffer represent the row being input. The
   *        remaining bytes must be exactly equal to {@linkplain #rowWidth()}.
   * @param promise
   *        declares how and whether the caller <em>agrees not to later modify</em> the
   *        given <tt>rows</tt> parameter. <em><strong>If the caller breaks the promise, then
   *        the table will almost certainly get corrupted!</strong></em>. May be <tt>NONE</tt>
   *        (no promise), but that would be a shame.
   */
  void setRow(ByteBuffer row, Covenant promise) throws IOException;

  /**
   * Inserts or updates the given <tt>rows</tt>. The rows are passed in <em>en bloc</em>:
   * they may be ordered in any way. The operation is fail-safe (all-or-nothing).
   * <p/>
   * Pay attention to the <tt>promise</tt> parameter. It has a huge impact on performance,
   * but just as importantly, if you break the promise, you break the data set.
   * 
   * @param rows
   *        the remaining bytes in this buffer represent the rows being input. The
   *        remaining bytes must be an exact multiple of the
   *        {@linkplain TStoreConfig#getRowWidth() row width}. If the
   *        same row (recall, the identity of a row is defined by the table's
   *        {@linkplain RowOrder}) occurs twice in this buffer, then the last occurance
   *        wins. 
   * @param promise
   *        declares how and whether the caller <em>agrees not to later modify</em> the
   *        given <tt>rows</tt> parameter. <em><strong>If the caller breaks the promise, then
   *        the table will almost certainly get corrupted!</strong></em>. May be <tt>NONE</tt>
   *        (no promise), but that would be a shame.
   */
  void setRows(ByteBuffer rows, Covenant promise) throws IOException;
  
  
  /**
   * Deletes the row with the given matching <tt>key</tt>, if any.
   */
  void deleteRow(ByteBuffer key) throws IOException;

}