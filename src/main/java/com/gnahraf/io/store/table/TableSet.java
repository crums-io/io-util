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
  
  protected final SortedTable[] tables;
  
  /**
   * Creates a new instance using the given backing <tt>table</tt>s. The order
   * of precedence in the given array is from back to front.
   */
  public TableSet(SortedTable[] tables) throws IOException {
    this(tables, true);
  }
  
  protected TableSet(SortedTable[] tables, boolean checkAndClone) throws IOException {
    
    if (checkAndClone) {
      
      if (tables == null)
        throw new IllegalArgumentException("null tables array");
      if (tables.length == 0)
        throw new IllegalArgumentException("empty tables array");
      
      if (tables[0] == null)
        throw new IllegalArgumentException("null table at index 0");
      
      this.tables = new SortedTable[tables.length];
      this.tables[0] = tables[0];
      checkTables(tables);
      for (int i = tables.length; i-- > 0; )
        this.tables[i] = tables[i];
    
    } else
      
      this.tables = tables;
  }
  
  
  public TableSet append(SortedTable table) throws IOException {
    checkTable(table);
    SortedTable[] set = new SortedTable[tables.length + 1];
    set[tables.length] = table;
    for (int i = tables.length; i-- > 0; )
      set[i] = tables[i];
    return new TableSet(set, false);
  }
  
  
  public TableSet append(SortedTable... table) throws IOException {
    checkTables(table);
    SortedTable[] set = new SortedTable[tables.length + table.length];
    for (int i = table.length; i-- > 0; )
      set[tables.length + i] = table[i];
    for (int i = tables.length; i-- > 0; )
      set[i] = tables[i];
    return new TableSet(set, false);
  }
  
  private void checkTables(SortedTable[] inputTables) throws IOException {
    for (int i = inputTables.length; i-- > 0; )
      checkTable(inputTables[i], i);
  }
  
  private void checkTable(SortedTable table, int atIndex) throws IOException {
    try {
      checkTable(table);
    } catch (IllegalArgumentException iax) {
      throw new IllegalArgumentException(iax.getMessage() + " -- at index " + atIndex);
    } catch (IOException iox) {
      throw new IOException(iox.getMessage() + " -- at index " + atIndex, iox);
    }
  }
  
  private void checkTable(SortedTable table) throws IOException {
    if (table == null)
      throw new IllegalArgumentException("null table");
    if (table.getRowWidth() != getRowWidth())
      throw new IllegalArgumentException(
          "row width mismatch. Expected " + getRowWidth() +
          "; actual was " + table.getRowWidth());
    if (!table.order().equals(order()))
      throw new IllegalArgumentException(
          "order mismatch. Expected " + order() + "; actual was " + table.order());
    if (!table.isOpen())
      throw new IllegalArgumentException("closed table: " + table);
  }
  
  
  public final int getRowWidth() {
    return tables[0].getRowWidth();
  }
  
  
  public final RowOrder order() {
    return tables[0].order();
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
