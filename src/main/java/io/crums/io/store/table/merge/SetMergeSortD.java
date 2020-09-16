/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.merge;

import java.io.IOException;
import java.util.Collections;

import io.crums.io.IoStateException;
import io.crums.io.store.table.SortedTable;
import io.crums.io.store.table.TableSet;
import io.crums.io.store.table.TableSetD;
import io.crums.io.store.table.del.DeleteCodec;

/**
 * A merge sort operation on <tt>SortedTable</tt>s with support for dealing with
 * deletion entries. Like it's parent class, this models a data structure with no duplicates.
 * 
 * @see DeleteCodec#isDeleted(java.nio.ByteBuffer)
 * 
 * @author Babak
 */
public class SetMergeSortD extends SetMergeSort {


  protected final DeleteCodec deleteCodec;
  protected final TableSet backSet;
  
  

  /**
   * Creates a new instance with the given delete codec and back-set. The delete codec
   * encapsulates the protocol for delete overrides. The back-set determines whether or not
   * any given delete entry in this merge should be preserved in the <tt>target</tt> of the
   * merge.
   * <p/>
   * The number of rows in each source
   * table's search buffer defaults to {@linkplain BaseMergeSort#DEFAULT_ROWS_PER_SEARCH_BUFFER
   * DEFAULT_ROWS_PER_SEARCH_BUFFER}.
   * 
   * @param sources
   *        the source tables ordered in increasing order of precedence. Each table contains
   *        unique rows (no 2 rows in a given table compare to 0 using the tables' row comparator).
   *        Results are very much undefined, o.w.
   * @param deleteCodec
   *        non-<tt>null</tt> delete codec
   * @param backSet
   *        optional back-set (may be <tt>null</tt>). Deletion entries in are checked against
   *        this back set in order to determine whether a given delete entry should purged
   *        (skipped) during the merge: if a given deletion entry overrides an entry in the
   *        back-set, then the deletion entry is preserved in the merge. Will typically be an
   *        of sub-type {@linkplain TableSetD}.
   */
  public SetMergeSortD(
      SortedTable target, SortedTable[] sources,
      DeleteCodec deleteCodec,
      TableSet backSet)
          throws IOException {
    this(target, sources, deleteCodec, backSet, DEFAULT_ROWS_PER_SEARCH_BUFFER);
  }


  /**
   * Creates a new instance with the given delete codec and back-set. The delete codec
   * encapsulates the protocol for delete overrides. The back-set determines whether or not
   * any given delete entry in this merge should be preserved in the <tt>target</tt> of the
   * merge.
   * 
   * @param sources
   *        the source tables ordered in increasing order of precedence. Each table contains
   *        unique rows (no 2 rows in a given table compare to 0 using the tables' row comparator).
   *        Results are very much undefined, o.w.
   * @param deleteCodec
   *        non-<tt>null</tt> delete codec
   * @param backSet
   *        optional back-set (may be <tt>null</tt>). Deletion entries are checked against
   *        this back set in order to determine whether a given delete entry should purged
   *        (skipped) during the merge: if a given deletion entry overrides an entry in the
   *        back-set, then the deletion entry is preserved in the merge. Will typically be an
   *        of sub-type {@linkplain TableSetD}.
   * @param searchBufferRowsPerTable
   *        the number of rows in each source table's search buffer
   *        
   * @see SetMergeSort#SetMergeSort(SortedTable, SortedTable[], int)
   */
  protected SetMergeSortD(
      SortedTable target, SortedTable[] sources,
      DeleteCodec deleteCodec,
      TableSet backSet,
      int searchBufferRowsPerTable)
          throws IOException {
    super(target, sources, searchBufferRowsPerTable);
    
    this.deleteCodec = deleteCodec;
    this.backSet = backSet;
    
    if (deleteCodec == null)
      throw new IllegalArgumentException("null deleteCodec");
  }
  

  @Override
  protected void processTop() throws IOException {
    
    // invariant: sources is sorted
    PrecedenceMergeSource top = sources.get(sources.size() - 1);
    PrecedenceMergeSource next = sources.get(sources.size() - 2);
    
    long blockEndRowNumber;
    long postTopRowNumber;
    
    // if *top* contains *next*s current row..
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
      
      
      
    } else if (deleteCodec.isDeleted(top.row()) && (backSet == null || backSet.getRow(top.row()) == null) ) {
      
      top.setRow(top.rowNumber() + 1);
      if (top.finished()) {
        sources.remove(sources.size() - 1);
        finishedSources.add(top);
      }
      // we wont be block copying
      blockEndRowNumber = postTopRowNumber = 0;
      
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
