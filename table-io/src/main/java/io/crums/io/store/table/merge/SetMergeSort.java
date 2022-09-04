/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.merge;


import java.io.IOException;
import java.util.Collections;

import io.crums.io.IoStateException;
import io.crums.io.store.table.SortedTable;

/**
 * A merge sort operation on <code>SortedTable</code>s, collectively representing a <em>set</em>.
 * That is, this models a data structure with no duplicates.
 * 
 * @see BaseMergeSource
 */
public class SetMergeSort extends BaseMergeSort<PrecedenceMergeSource> {

  /**
   * Creates a new instance with the given <code>sources</code>, that will merge to the specified
   * <code>target</code>. The precedence of the tables is from back to front. That is, higher index
   * tables in the given array override lower index tables.
   * <p>
   * The number of rows in each source
   * table's search buffer defaults to {@linkplain BaseMergeSort#DEFAULT_ROWS_PER_SEARCH_BUFFER
   * DEFAULT_ROWS_PER_SEARCH_BUFFER}.
   * </p>
   * 
   * @param sources
   *        the source tables ordered in increasing order of precedence. Each table contains
   *        unique rows (no 2 rows in a given table compare to 0 using the tables' row comparator).
   *        Results are very much undefined, o.w.
   */
  public SetMergeSort(SortedTable target, SortedTable[] sources) throws IOException {
    this(target, sources, DEFAULT_ROWS_PER_SEARCH_BUFFER);
  }


  /**
   * Creates a new instance with the given <code>sources</code>, that will merge to the specified
   * <code>target</code>. The precedence of the tables is from back to front. That is, higher index
   * tables in the given array override lower index tables.
   * 
   * @param sources
   *        the source tables ordered in increasing order of precedence. Each table contains
   *        unique rows (no 2 rows in a given table compare to 0 using the tables' row comparator).
   *        Results are very much undefined, o.w.
   * @param searchBufferRowsPerTable
   *        the number of rows in each source table's search buffer
   */
  public SetMergeSort(SortedTable target, SortedTable[] sources, int searchBufferRowsPerTable) throws IOException {
    super(target, sources, searchBufferRowsPerTable);
  }


  @Override
  protected PrecedenceMergeSource newMergeSource(
      SortedTable table, int searchBufferRowsPerTable, int tableIndex) throws IOException {
    return new PrecedenceMergeSource(table.newSearcher(searchBufferRowsPerTable), tableIndex);
  }


  @Override
  protected void processTop() throws IOException {
    
    // invariant: sources is sorted
    PrecedenceMergeSource top = sources.get(sources.size() - 1);
    PrecedenceMergeSource next = sources.get(sources.size() - 2);
    
    long blockEndRowNumber;
    long postTopRowNumber;
    
    // see if *top* contains *next*s current row: if so, advance next and re-sort,
    // and soon-come-back!
    
    if (top.searcher().search(next.row())) {
      
      // if top overrides next's row..
      if (top.precedence() > next.precedence()) {
        // skip next's row and if it's then finished, removed it
        next.setRow(next.rowNumber() + 1);
        if (next.finished()) {
          sources.remove(sources.size() - 2);
          finishedSources.add(next);
        }
        // we wont be block copying
        blockEndRowNumber = postTopRowNumber = 0;
        
      } else {
        // o.w. next's row overrides a row in top..
        // we'll kill (skip) that row in top (hence the gap in
        // blockEndRowNumber and postTopRowNumber) and do the
        // block copy from top to target
        blockEndRowNumber = top.searcher().getHitRowNumber();
        postTopRowNumber = blockEndRowNumber + 1;
      }
      
      
      
    } else {
      // Good. next row is not found in *top*, so we're ready to block-copy
      // from *top* to *target*
      
      blockEndRowNumber = -top.searcher().getHitRowNumber() - 1;
      postTopRowNumber = blockEndRowNumber;
    }
    
    // if we're set up for block copy..
    if (blockEndRowNumber != 0) {
      long count = blockEndRowNumber - top.rowNumber();
      if (count < 1)
        throw new IoStateException("assertion failure: count=" + count);
      
      target.appendRows(top.table(), top.rowNumber(), count);
      
      top.setRow(postTopRowNumber);
      
      // if top is finished
      if (top.finished()) {
        sources.remove(sources.size() - 1);
        finishedSources.add(top);
      }
    }
    
    // Sort the sources (we messed with sources)
    // and satisfy the required post-condition for this method
    Collections.sort(sources);
  }
  

 
}
