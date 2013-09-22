/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.gnahraf.io.store.table.SortedTable.Searcher;
import com.gnahraf.io.store.table.order.RowOrder;

/**
 * 
 * @author Babak
 */
public class TableSet {
  
  private final SortedTable[] tables;
  
  public TableSet(SortedTable[] tables) throws IOException {
    this(tables, true);
  }
  
  public TableSet(SortedTable[] tables, boolean checkAndClone) throws IOException {
    
    if (checkAndClone) {
      if (tables == null)
        throw new IllegalArgumentException("null tables array");
      if (tables.length == 0)
        throw new IllegalArgumentException("empty tables array");

      final int rowWidth = tables[0].getRowWidth();
      final RowOrder order = tables[0].order();
      
      SortedTable[] copy = new SortedTable[tables.length];
      for (int i = tables.length; i-- > 0; ) {
        copy[i] = tables[i];
        if (copy[i] == null)
          throw new IllegalArgumentException("null table at index " + i);
        if (copy[i].isEmpty())
          throw new IllegalArgumentException("empty table at index " + i + "; " + copy[i]);
        if (!copy[i].isOpen())
          throw new IllegalArgumentException("closed table at index " + i + "; " + copy[i]);
        if (copy[i].getRowWidth() != rowWidth)
          throw new IllegalArgumentException(
              "row width mismatch at index " + i + ": expected " + rowWidth +
              " but found " + copy[i].getRowWidth());
        if (!copy[i].order().equals(order))
          throw new IllegalArgumentException(
              "row order mismatch at index " + i + ": expected " + order +
              " but found " + copy[i].order());
      }
      tables = copy;
    }
    
    this.tables = tables;
  }
  
  
  public final int getRowWidth() {
    return tables[0].getRowWidth();
  }
  
  
  public ByteBuffer getRow(ByteBuffer key) throws IOException {
    for (int i = tables.length; i-- > 0; ) {
      Searcher searcher = getSearcher(tables[i]);
      if (searcher.search(key)) {
        return searcher.getHitRow();
      }
    }
    return null;
  }

  private final static int SEARCH_BUFFER_SIZE_LIMIT = 8192;

  
  
  protected Searcher getSearcher(SortedTable table) throws IOException {
    return table.newSearcher(SEARCH_BUFFER_SIZE_LIMIT / getRowWidth());
  }
  

}
