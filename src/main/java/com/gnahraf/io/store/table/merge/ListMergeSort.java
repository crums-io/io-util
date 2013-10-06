/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.merge;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import com.gnahraf.io.IoStateException;
import com.gnahraf.io.store.NotSortedException;
import com.gnahraf.io.store.table.SortedTable;

/**
 * A merge sort operation on <tt>SortedTable</tt>s. The <tt>SortedTable</tt>s may contain
 * duplicate (or equally ranked) rows, in which case all duplicates will be preserved.
 * <p/>
 * <h4>FIXME</h4>
 * The dupe logic is broken. There are edge cases where some duplicate rows may be dropped.
 * (I haven't written the failing test or encountered the bug, but can reason how it can
 * happen.)
 * 
 * @author Babak
 */
public class ListMergeSort extends BaseMergeSort<ListMergeSource> {
  
  private int compZeroEdgeCase;
  
  
  /**
   * Creates a new instance with {@linkplain #DEFAULT_ROWS_PER_SEARCH_BUFFER}.
   */
  public ListMergeSort(SortedTable target, SortedTable[] sources) throws IOException {
    
    this(target, sources, DEFAULT_ROWS_PER_SEARCH_BUFFER);
  }
  
  
  /**
   * @param searchBufferRowsPerTable
   *        the number of rows in the per-table search buffer
   */
  public ListMergeSort(
      SortedTable target, SortedTable[] sources, int searchBufferRowsPerTable)
          throws IOException {
    
    super(target, sources, searchBufferRowsPerTable);
  }
  
  
  
  @Override
  protected void processTop() throws IOException {
    
    // invariant: sources is sorted
    ListMergeSource top = sources.get(sources.size() - 1);
    ListMergeSource next = sources.get(sources.size() - 2);
    
    final long blockEndRowNumber; // exclusive
    
    // search for next.row() in top..
    
    // if found
    if (top.searcher().search(next.row())) {
      
      // next.row() was found in the top merge source..
      long rowNumberCursor = top.searcher().getHitRowNumber();  // +

      // search for the last row in the top merge source that matches next.row()..
      
      // first search the contents search buffer..
      while (++rowNumberCursor < top.searcher().getLastRetrievedRowNumber()) {
        int comp = top.searcher().compareToRetrievedRow(next.row(), rowNumberCursor);
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
      if (rowNumberCursor == top.searcher().getLastRetrievedRowNumber()) {
        
        ByteBuffer sampleRow = ByteBuffer.allocate(top.table().getRowWidth());
        
        while (rowNumberCursor < top.rowCount()) {
          
          sampleRow.clear();
          top.table().read(rowNumberCursor, sampleRow);
          sampleRow.flip();
          
          int comp = top.table().order().compareRows(next.row(), sampleRow);
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
      // the last row number (exclusive) we'll block copy is the row number we would have
      // found next.row() were it existed in top
      blockEndRowNumber = -top.searcher().getHitRowNumber() - 1;
    
    long count = blockEndRowNumber - top.rowNumber();
    if (count < 1)
      throw new IoStateException("assertion failure: count=" + count);
    
    target.appendRows(top.table(), top.rowNumber(), count);
    
    // advance top's row number to the end of the block we just copied
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
  


  public final int getCompZeroEdgeCase() {
    return compZeroEdgeCase;
  }


  @Override
  protected ListMergeSource newMergeSource(SortedTable table, int searchBufferRowsPerTable, int tableIndex) throws IOException {
    return new ListMergeSource(table.newSearcher(searchBufferRowsPerTable));
  }
  
  

}
