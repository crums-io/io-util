/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import io.crums.io.store.table.SortedTable;

/**
 * 
 * @author Babak
 */
public abstract class BaseMergeSort<M extends BaseMergeSource<M>> {
  

  public final static int DEFAULT_ROWS_PER_SEARCH_BUFFER = 64;
  
  
  
  
  
  
  protected final SortedTable target;
  
  /**
   * Sorted list of merge sources.
   */
  protected final ArrayList<M> sources;
  /**
   * For bookkeeping only. Unused. Once the entire contents of a
   * source has been written to the target, the source is placed
   * in here and removed from <code>sources</code>.
   */
  protected final ArrayList<M> finishedSources;
  
  protected long startTime;
  protected long endTime;
  
  private boolean abort;
  
  
  public BaseMergeSort(
      SortedTable target, SortedTable[] sources, int searchBufferRowsPerTable)
          throws IOException {
    
    this.target = target;
    
    if (target == null)
      throw new IllegalArgumentException("null target");
    if (sources == null)
      throw new IllegalArgumentException("null merge tables array");
    if (!target.isOpen())
      throw new IllegalArgumentException("target not open");
    if (searchBufferRowsPerTable < SortedTable.Searcher.MIN_BUFFER_ROWS)
      throw new IllegalArgumentException(
          "min expected searchBufferRowsPerTable is " + SortedTable.Searcher.MIN_BUFFER_ROWS +
          "; actual was " + searchBufferRowsPerTable);
    
    if (sources.length < 2)
      throw new IllegalArgumentException("too few tables in array: " + sources.length);
    
    this.sources = new ArrayList<>(sources.length);
    for (int i = 0; i < sources.length; ++i) {
      if (target.getRowWidth() != sources[i].getRowWidth())
        throw new IllegalArgumentException(
            "source / target row width mismatch: " +
            sources[i].getRowWidth() + " / " + target.getRowWidth() +
            "  -- at index " + i);
      if (!target.order().equals(sources[i].order()))
        throw new IllegalArgumentException(
            "source / target order mismatch: " +
            sources[i].order() + " / " + target.order() +
            "  -- at index " + i);
      M mergeSource = newMergeSource(sources[i], searchBufferRowsPerTable, i);
      this.sources.add(mergeSource);
    }

    Collections.sort(this.sources);
    finishedSources = new ArrayList<>(sources.length);
  }
  
  
  protected abstract M newMergeSource(
      SortedTable table, int searchBufferRowsPerTable, int tableIndex) throws IOException;

  

  
  
  /**
   * Tests whether the {@linkplain #abort() abort} method has been called in the middle
   * of a merge. (This says nothing about whether {@linkplain #mergeToTarget()} has
   * returned.)
   */
  public boolean isAborted() {
    return abort;
  }
  
  /**
   * Aborts a merge, unless of course no merge is no progress.
   * 
   * @return <code>true</code>, if aborted an in-progress merge; <code>false</code>, o.w.
   */
  public boolean abort() {
    if (startTime == 0 || endTime != 0)
      return false;
    abort = true;
    return true;
  }


  public final SortedTable getTarget() {
    return target;
  }
  
  
  public boolean isFinished() {
    return endTime != 0;
  }
  
  public boolean isStarted() {
    return startTime != 0;
  }


  /**
   * Returns the time (UTC millis) the merge started; zero, if not started.
   */
  public final long getStartTime() {
    return startTime;
  }


  /**
   * Returns the time (UTC millis) the merge finished; zero, if not finished.
   */
  public final long getEndTime() {
    return endTime;
  }
  
  /**
   * Returns the total time elapsed for the {@linkplain #mergeToTarget()} merge;
   * zero, if not started.
   */
  public final long getTimeTaken() {
    if (startTime == 0)
      return 0;
    long endMillis = endTime == 0 ? System.currentTimeMillis() : endTime;
    return endMillis - startTime;
  }
  

  
  
  /**
   * Merges the sources to the target table.
   */
  public void mergeToTarget() throws IOException {
    
    synchronized (sources) {
      if (startTime != 0)
        throw new IllegalStateException("already run");
      startTime = System.currentTimeMillis();
    }
    
    while (sources.size() > 1 && !abort) {
      // assumed invariant: sources are sorted
      processTop();
    }
    
    if (!abort && !sources.isEmpty()) {
      M last = sources.get(0);
      target.appendRows(
          last.table(), last.rowNumber(), last.rowCount() - last.rowNumber());
    }
    
    endTime = System.currentTimeMillis();
  }
  
  
  /**
   * Processes the top of the <code>MergeSource</code> stack. If any of the merge
   * {@linkplain #sources} is {@linkplain BaseMergeSource#finished() finished}
   * then that it should be removed from the {@linkplain sources} list and
   * placed in the {@linkplain #finishedSources} list.
   * <h4>Pre-condition</h4>
   * <ul><li>
   * {@linkplain #sources} is sorted
   * </li><li>
   * {@linkplain #sources} has at least 2 elements
   * </li></ul>
   * <h4>Post-condition</h4>
   * <ul><li>
   * {@linkplain #sources} is sorted
   * </li><li>
   * {@linkplain #sources} contains no {@linkplain BaseMergeSource#finished() finished}
   * instances.
   * </li></ul>
   * 
   * @throws IOException
   */
  protected abstract void processTop() throws IOException;
  
}
