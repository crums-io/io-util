/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import com.gnahraf.io.IoStateException;
import com.gnahraf.io.store.NotSortedException;
import com.gnahraf.io.store.table.SortedTable.Searcher;

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
      
      final long blockEndRowNumber; // exclusive
      
      // search for next.row()..
      if (top.searcher.search(next.row())) {
        
        // next.row() was found in the top merge source..
        long rowNumberCursor = top.searcher.getHitRowNumber();  // +

        // search for the last row in the top merge source that matches next.row()..
        
        // first search the contents search buffer..
        while (++rowNumberCursor < top.searcher.getLastRetrievedRowNumber()) {
          int comp = top.searcher.compareToRetrievedRow(next.row(), rowNumberCursor);
          if (comp < 0)
            break;
          else if (comp > 0)
            throw new NotSortedException("at row number " + rowNumberCursor);
          // comp == 0
        }
        
        // if our cursor ran beyond the contents of the search buffer,
        // continue a linear on the top merge source's table.
        // This _can_ be sped up, but very long sequences of dups are hopefully infrequent,
        // so we're not bothering with optimizing
        //
        if (rowNumberCursor == top.searcher.getLastRetrievedRowNumber()) {
          
          ByteBuffer sampleRow = ByteBuffer.allocate(top.table.getRowWidth());
          
          while (rowNumberCursor < top.table.getRowCount()) {
            
            sampleRow.clear();
            top.table.read(rowNumberCursor, sampleRow);
            sampleRow.flip();
            
            int comp = top.table.order().compareRows(next.row(), sampleRow);
            if (comp < 0)
              break;
            else if (comp > 0)
              throw new NotSortedException("at row number " + rowNumberCursor);
            ++rowNumberCursor;
          }
        }
        
        blockEndRowNumber = rowNumberCursor;
        
      } else
        // next.row() is NOT found in top merge source
        blockEndRowNumber = -top.searcher.getHitRowNumber() - 1;
      
      long count = blockEndRowNumber - top.rowNumber();
      if (count < 1)
        throw new IoStateException("assertion failure: count=" + count);
      
      target.appendRows(top.table, top.rowNumber(), count);
      
      top.setRow(blockEndRowNumber);
      
      if (top.finished()) {
        
        sources.remove(sources.size() - 1);
        finishedSources.add(top);
      
      } else {
        int comp = top.compareTo(next);
        if (comp < 0)
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
