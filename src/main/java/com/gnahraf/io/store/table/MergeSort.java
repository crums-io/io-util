/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.gnahraf.io.store.NotSortedException;
import com.gnahraf.io.store.table.SortedTable.Searcher;
import com.gnahraf.io.store.table.order.RowOrder;

/**
 * A merge sort operation on <tt>SortedTable</tt>s.
 * 
 * @author Babak
 */
public class MergeSort {
  

  public final static int DEFAULT_ROWS_PER_SEARCH_BUFFER = 64;
  
  
  /**
   * We maintain a stack of these. Each instance encapsulates progress
   * from a single table. They are mutually comparable in a way that allows
   * us to easily track rows from which source table must be copied to the
   * target of the merge.
   * 
   * @author Babak
   */
  class MergeSource implements Comparable<MergeSource> {


    private final SortedTable table;
    private final Searcher searcher;
    private final long rowCount;
    private final ByteBuffer row;
    private long rowCursor;
    
    MergeSource(SortedTable.Searcher searcher) throws IOException {
      if (searcher == null)
        throw new IllegalArgumentException("null searcher");
      this.table = searcher.getTable();
      this.searcher = searcher;
      this.rowCount = table.getRowCount();
      if (rowCount == 0)
        throw new IllegalArgumentException("empty table: " + table);
      
      if (table.getRowWidth() != target.getRowWidth())
        throw new IllegalArgumentException(
            "row size mismatch: " + table.getRowWidth() + "/" + target.getRowWidth() +
            " (table/target)");
      
      if (!target.order().equals(table.order())) {
        throw new IllegalArgumentException(
            "ordering mismatch: target.order=" + target.order() +
            "; source.order=" + table.order());
      }

      this.row = ByteBuffer.allocate(table.getRowWidth());
      setRow(0);
      row.clear();
      table.read(0, row);
      row.flip();
    }
    
    /**
     * Returns the row number of the current {@linkplain #row()}.
     */
    long rowNumber() {
      return rowCursor;
    }
    
    /**
     * Sets the {@linkplain #rowNumber()} and loads the corresponding {@linkplain #row()};
     * unless the <tt>rowNum</tt> argument is equal to {@linkplain #getRowCount()}.
     * 
     * @throws IndexOutOfBoundsException
     */
    void setRow(long rowNum) throws IOException {
      if (rowNum >= rowCount) {
        if (rowNum == rowCount) {
          rowCursor = rowCount;
          return;
        }
        throw new IndexOutOfBoundsException("rowNum/rowCount: " + rowNum + "/" + rowCount);
      }
      row.clear();
      table.read(rowNum, row);
      row.flip();
      rowCursor = rowNum;
    }
    
    /**
     * Returns the snapshot row count. (If there are concurrent additions to the
     * underlying table, those will be ignored.)
     * @return
     */
    long rowCount() {
      return rowCount;
    }

    
    /**
     * Returns the contents of the current {@linkplain #rowNumber()}, unless the
     * instance is {@linkplain #finished()}.
     * @return
     */
    ByteBuffer row() {
      return row;
    }
    
    /**
     * An instance is finished when all its rows have been copied to the target. I.e. when
     * <tt>{@linkplain #rowNumber()} == {@linkplain #rowCount()}.
     */
    boolean finished() {
      return rowCursor == rowCount;
    }

    
    /**
     * Instances are sorted in reverse order of their row content. (An instance always
     * has row contents for <em>some</em> row, unless it's {@linkplain #finished()}.
     */
    @Override
    public int compareTo(MergeSource other) {
      // we're removing finished instances from our sorted list
      // so might as well, assert some sanity here..
      if (this.finished() || other.finished()) {
        throw new RuntimeException(
            "assertion failure: this/other is finished. this=" + this + "; other=" + other);
      }
      
      return -target.order().compare(this.row, other.row);
//      if (this.finished())
//        return other.finished() ? 0 : -1;
//      else if (other.finished())
//        return 1;
//      else
//        return -order.compare(this.row, other.row);
    }
    
    

    @Override
    public String toString() {
      return
          "MergeSource[" +
          "table=" + table +
          ", row=" + row +
          ", rowCount=" + rowCount +
          ", rowNumber()=" + rowNumber() + "]";
    }
    
  }
  
  
  
  
  
  
  
  
  private final SortedTable target;
  
  private final ArrayList<MergeSource> sources;
  private final ArrayList<MergeSource> finishedSources;
  
  private long startTime;
  private long endTime;
  
  private int compZeroEdgeCase;
  
  
  /**
   * Creates a new instance
   */
  public MergeSort(
      SortedTable target, SortedTable[] tables)
          throws IOException {
    
    this(target, tables, DEFAULT_ROWS_PER_SEARCH_BUFFER);
  }
  
  
  public MergeSort(
      SortedTable target, SortedTable[] tables, int searchBufferRowsPerTable)
          throws IOException {
    
    this.target = target;
    
    if (target == null)
      throw new IllegalArgumentException("null target");
    if (tables == null)
      throw new IllegalArgumentException("null merge tables array");
    if (!target.isOpen())
      throw new IllegalArgumentException("target not open");
    if (searchBufferRowsPerTable < SortedTable.Searcher.MIN_BUFFER_ROWS)
      throw new IllegalArgumentException(
          "min expected searchBufferRowsPerTable is " + SortedTable.Searcher.MIN_BUFFER_ROWS +
          "; actual was " + searchBufferRowsPerTable);
    
    if (tables.length < 2)
      throw new IllegalArgumentException("too few tables in array: " + tables.length);
    
    sources = new ArrayList<>(tables.length);
    for (int i = 0; i < tables.length; ++i) {
      try {
        sources.add(new MergeSource(tables[i].newSearcher(searchBufferRowsPerTable)));
      } catch (IllegalArgumentException iax) {
        throw new IllegalArgumentException("at index [" + i + "]: " + iax.getMessage());
      }
    }
    
    Collections.sort(sources);
    finishedSources = new ArrayList<>(tables.length);
  }
  
  
  public void mergeToTarget() throws IOException {
    synchronized (sources) {
      if (startTime != 0)
        throw new IllegalStateException("already run");
      startTime = System.currentTimeMillis();
    }
    while (sources.size() > 1) {
      // invariant: sources is sorted
      MergeSource top = sources.get(sources.size() - 1);
      MergeSource next = sources.get(sources.size() - 2);
      
      long blockEndRowNumber; // exclusive
      if (top.searcher.search(next.row()))
        blockEndRowNumber = top.searcher.getHitRowNumber() + 1;
      else
        blockEndRowNumber = -top.searcher.getHitRowNumber() - 1;
      
      long count = blockEndRowNumber - top.rowNumber();
      target.appendRows(top.table, top.rowNumber(), count);
      
      top.setRow(top.rowNumber() + count);
      
      if (top.finished()) {
        sources.remove(sources.size() - 1);
        finishedSources.add(top);
      }
      else {
        int comp = top.compareTo(next);
        if (top.compareTo(next) < 0)
          // sort the sources
          Collections.sort(sources);
        else if (comp == 0)
          ++compZeroEdgeCase;
        else
          throw new NotSortedException("unsorted table: " + top);
      }
      
    }
    
    if (sources.size() != 1)
      throw new RuntimeException("assertion failure: " + sources.size());
    
    MergeSource last = sources.get(0);
    target.appendRows(last.table, last.rowNumber(), last.rowCount() - last.rowNumber());
    
    endTime = System.currentTimeMillis();
  }


  public final SortedTable getTarget() {
    return target;
  }


  public final long getStartTime() {
    return startTime;
  }


  public final long getEndTime() {
    return endTime;
  }


  public final int getCompZeroEdgeCase() {
    return compZeroEdgeCase;
  }
  
  

}
