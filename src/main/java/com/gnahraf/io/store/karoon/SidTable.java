/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon;


import java.io.IOException;
import java.nio.channels.FileChannel;

import com.gnahraf.io.store.table.SortedTable;
import com.gnahraf.io.store.table.TableSet;
import com.gnahraf.io.store.table.order.RowOrder;

/**
 * A <tt>SortedTable</tt> tagged with a bookkeeping ID.
 * 
 * @author Babak
 */
public class SidTable extends SortedTable {
  
  private final long id;

  /**
   * Creates a new instance by loading it from the given <tt>file</tt> channel,
   * at the file's current position.
   * 
   * @see SortedTable#SortedTable(FileChannel, int, RowOrder)
   */
  public SidTable(FileChannel file, int rowSize, RowOrder order, long id) throws IOException {
    super(file, rowSize, order);
    this.id = id;
  }


  /**
   * Creates a new instance by loading it at the specified <tt>zeroRowFileOffset</tt>.
   * 
   * @see SortedTable#SortedTable(FileChannel, long, int, RowOrder)
   */
  public SidTable(
      FileChannel file, long zeroRowFileOffset, int rowSize, RowOrder order, long id)
          throws IOException {
    super(file, zeroRowFileOffset, rowSize, order);
    this.id = id;
  }
  
  private SidTable(SortedTable table, long id) {
    super(table);
    this.id = id;
  }
  
  /**
   * Returns the table's ID.
   */
  public final long id() {
    return id;
  }
  
  
  
  @Override
  public SidTable sliceTable(long firstRow, long count) throws IOException {
    SortedTable table = super.sliceTable(firstRow, count);
    return new SidTable(table, id);
  }
  
  
  /**
   * Returns the table's decimal ID.
   */
  @Override
  public String toString() {
    return Long.toString(id);
  }

  

  /**
   * Returns the same searcher. Reminder: <em>no concurrent access!</em>.
   * @return
   * @throws IOException
   */
  public Searcher getSearcher() throws IOException {
    if (searcher == null) {
      long byteSize = getRowCount() * getRowWidth();
      int rowsInBuffer;
      // if the table size is <= 16k, load it all into memory
      if (byteSize <= 2 * TableSet.DEFAULT_SEARCH_BUFFER_SIZE)
        rowsInBuffer = (int) getRowCount();
      else
        rowsInBuffer = TableSet.DEFAULT_SEARCH_BUFFER_SIZE / getRowWidth();
      rowsInBuffer = Math.max(rowsInBuffer, 8);
      searcher = newSearcher(rowsInBuffer);
    }
    return searcher;
  }
  
  private Searcher searcher;
  
  

}
