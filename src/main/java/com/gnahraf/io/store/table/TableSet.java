/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.gnahraf.io.store.Sorted;
import com.gnahraf.io.store.table.SortedTable.Searcher;
import com.gnahraf.io.store.table.iter.TableSetIterator;
import com.gnahraf.io.store.table.order.RowOrder;
import com.gnahraf.util.CollectionUtils;
import com.gnahraf.util.TaskStack;

/**
 * A stack of {@linkplain SortedTable SortedTable}s, the top overriding the bottom.
 * 
 * @author Babak
 */
public class TableSet implements Sorted, Closeable {
  
  private final Logger LOG = Logger.getLogger(TableSet.class);
  
  protected final static SortedTable[] EMPTY_TABLE_ARRAY = { };
  
  protected final SortedTable[] tables;

  private final RowOrder order;
  private final int rowWidth;
  
  
  /**
   * Creates an empty instance.
   */
  public TableSet(RowOrder order, int rowWidth) {
    this.tables = EMPTY_TABLE_ARRAY;
    this.order = order;
    this.rowWidth = rowWidth;
    
    if (order == null)
      throw new IllegalArgumentException("null row order");
    if (rowWidth < 1)
      throw new IllegalArgumentException("row width: " + rowWidth);
  }
  
  /**
   * Creates a new instance using the given backing <tt>tables</tt>. The order
   * of precedence in the given array is from back to front. Each <tt>SortedTable</tt>
   * element of the given array is assumed to not contain any rows with duplicate keys.
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
      order = tables[0].order();
      rowWidth = tables[0].getRowWidth();
      
      checkTables(tables);
      for (int i = tables.length; i-- > 0; )
        this.tables[i] = tables[i];
    
    } else {
      this.tables = tables;
      order = tables[0].order();
      rowWidth = tables[0].getRowWidth();
    }
  }
  
  
  /**
   * Returns a new iterator positioned at the given search <tt>key</tt>.
   */
  public TableSetIterator iterator() throws IOException {
    return new TableSetIterator(this);
  }
  
  
  public TableSet append(SortedTable table) throws IOException {
    SortedTable[] set = appendImpl(table);
    return new TableSet(set, false);
  }
  
  
  protected SortedTable[] appendImpl(SortedTable table) throws IOException {
    checkTable(table);
    SortedTable[] set = new SortedTable[tables.length + 1];
    set[tables.length] = table;
    for (int i = tables.length; i-- > 0; )
      set[i] = tables[i];
    return set;
  }
  
  
  public TableSet append(SortedTable... table) throws IOException {
    SortedTable[] set = appendImpl(table);
    return new TableSet(set, false);
  }
  
  protected SortedTable[] appendImpl(SortedTable[] table) throws IOException {
    checkTables(table);
    SortedTable[] set = new SortedTable[tables.length + table.length];
    for (int i = table.length; i-- > 0; )
      set[tables.length + i] = table[i];
    for (int i = tables.length; i-- > 0; )
      set[i] = tables[i];
    return set;
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
  
  protected final void checkTable(SortedTable table) throws IOException {
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
    return rowWidth;
  }
  
  
  public final RowOrder order() {
    return order;
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
  
  
  public List<SortedTable> tables() {
    return CollectionUtils.asReadOnlyList(tables);
  }
  
  
  
  
  

  public final static int DEFAULT_SEARCH_BUFFER_SIZE = 8192;

  
  
  protected Searcher getSearcher(SortedTable table) throws IOException {
    return table.newSearcher(DEFAULT_SEARCH_BUFFER_SIZE / getRowWidth());
  }


  /**
   * Closes the underlying tables. The base implementation does not throw
   * any exceptions.
   */
  @Override
  public void close() throws IOException {
    TaskStack closer = new TaskStack(LOG);
    closer.pushClose(tables);
    closer.close();
  }
  

  @Override
  public String toString() {
    return tables().toString();
  }
}
