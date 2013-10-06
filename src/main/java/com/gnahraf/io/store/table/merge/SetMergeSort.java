/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.merge;

import java.io.IOException;
import java.util.Collections;

import com.gnahraf.io.IoStateException;
import com.gnahraf.io.store.table.SortedTable;

/**
 * A merge sort operation on <tt>SortedTable</tt>s, collectively representing a <em>set</em>.
 * That is, this model's a data structure with no duplicates.
 * 
 * @author Babak
 */
public class SetMergeSort extends BaseMergeSort<PrecedenceMergeSource> {

  /**
   * Creates a new instance with the given <tt>sources</tt>, that will merge to the specified
   * <tt>target</tt>. The precedence of the tables is from back to front. That is, higher index
   * tables in the given array override lower index tables. The number of rows in each source
   * table's search buffer defaults to {@linkplain BaseMergeSort#DEFAULT_ROWS_PER_SEARCH_BUFFER
   * DEFAULT_ROWS_PER_SEARCH_BUFFER}. 
   */
  public SetMergeSort(SortedTable target, SortedTable[] sources) throws IOException {
    this(target, sources, DEFAULT_ROWS_PER_SEARCH_BUFFER);
  }


  /**
   * Creates a new instance with the given <tt>sources</tt>, that will merge to the specified
   * <tt>target</tt>. The precedence of the tables is from back to front. That is, higher index
   * tables in the given array override lower index tables. The number of rows in each source
   * table's search buffer defaults will be <tt>searchBufferRowsPerTable</tt>. 
   */
  public SetMergeSort(SortedTable target, SortedTable[] sources, int searchBufferRowsPerTable) throws IOException {
    super(target, sources, searchBufferRowsPerTable);
  }


  @Override
  protected PrecedenceMergeSource newMergeSource(SortedTable table, int searchBufferRowsPerTable, int tableIndex) throws IOException {
    return new PrecedenceMergeSource(table.newSearcher(searchBufferRowsPerTable), tableIndex);
  }


  @Override
  protected void processTop() throws IOException {
    
    // invariant: sources is sorted
    PrecedenceMergeSource top = sources.get(sources.size() - 1);
    PrecedenceMergeSource next = sources.get(sources.size() - 2);
    
    // see if *top* is overriding *next*: if so, advance next and re-sort,
    // and soon-come-back!
    
    if (top.searcher().search(next.row())) {
      // next row was also found in top: therefore top overrides next
      
      // Advance *next* and if it's then finished, removed it
      next.setRow(next.rowNumber() + 1);
      if (next.finished()) {
        sources.remove(sources.size() - 2);
        finishedSources.add(next);
      }
    } else {
      // Good. next row is not found in *top*, so we're ready to block-copy
      // from *top* to *target*
      
      long blockEndRowNumber = -top.searcher().getHitRowNumber() - 1;
      long count = blockEndRowNumber - top.rowNumber();
      if (count < 1)
        throw new IoStateException("assertion failure: count=" + count);
      
      target.appendRows(top.table(), top.rowNumber(), count);
      
      top.setRow(blockEndRowNumber);
      
      if (top.finished()) {
        sources.remove(sources.size() - 1);
        finishedSources.add(top);
      }
    }
    
    Collections.sort(sources);
  }
  

 
}
