/*
 * Copyright 2014 Babak Farhang 
 */
package com.gnahraf.io.store.karoon.merge;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gnahraf.io.Releaseable;
import com.gnahraf.io.store.karoon.CommitRecord;
import com.gnahraf.util.CollectionUtils;

/**
 * Used to mediate access to tables. This mediation achieves 2 things:
 * <ol>
 * <li>Disallows the same table being the source of multiple concurrent merge operations.</li>
 * <li>Tracks table usage and notifies when a table may be safely deleted.</li>
 * </ol>
 * 
 * @author Babak
 */
public class TableRegistry {
  
  
  private final Object lock = this;
  private final Set<Long> srcIds = new HashSet<>();
  private final Map<Long, int[]> tableRefCounts = new HashMap<>();
  private final TableLifecycleListener lifecycleListener;
  private final boolean strict;
  private CommitRecord latestCommit = CommitRecord.INIT;
  
  
  
  
  
  
  public TableRegistry(TableLifecycleListener lifecycleListener) {
    this(lifecycleListener, false);
  }
  
  
  public TableRegistry(TableLifecycleListener lifecycleListener, boolean lenient) {
    if (lifecycleListener == null)
      throw new IllegalArgumentException("null lifecycleListener");
    
    this.lifecycleListener = lifecycleListener;
    this.strict = !lenient;
  }
  
  
  
  
  /**
   * Attempts to advance the "last-seen-commit" to the specified <tt>commit</tt> record. This
   * method succeeds if the given commit's ID is &ge; the last-seen-commit (that is, at or more recent).
   * 
   * @param commit
   * @return <tt>true</tt> if the given <tt>commit</tt> is at or later than the <em>last seen
   *         commit</em> (i.e. if {@code commit.getId() >= latestCommit.getId()}
   */
  public boolean advanceCommit(CommitRecord commit) {
    synchronized (lock) {
      if (commit.getId() > latestCommit.getId()) {
        CommitRecord oldCommit = latestCommit;
        latestCommit = commit;
        addRefs(latestCommit.getTableIds());
        releaseRefs(oldCommit.getTableIds());
        return true;
      } else {
        return commit.getId() == latestCommit.getId();
      }
    }
  }
  
  /**
   * Attempts to check out the given tables for a merge. On success a {@linkplain Releaseable}
   * object is returned; if the check out fails, <tt>null</tt> is returned.
   * <p/>
   * A source table (identified by its ID) may only ever be checked out by a single (unclosed)
   * <tt>Releaseable</tt> instance. There's no such restriction on the backset tables used by
   * the merge.
   * <p/>
   * The <tt>sourceTableIds</tt> and <tt>backSetTableIds</tt> are assumed to come from the 
   * <tt>commit</tt> record ({@linkplain CommitRecord#getTableIds()}. This requirement is not
   * enforced, but failure to observe it will break the reference counting--as well as
   * possibly other undefined behavior.
   * 
   * @param sourceTableIds
   *        the IDs of the source tables. The caller agrees not to modify the list.
   * @param backSetTableIds
   *        the IDS of the tables in the backset. The caller agrees not to modify the list.
   * @param commit
   *        the commit snapshot at which this check out occurs. ({@linkplain #advanceCommit(CommitRecord)
   *        advanceCommit} is first invoked in the same atomic operation.)
   * 
   * @throws NullPointerException
   *         if any of the arguments is null
   */
  public Releaseable checkOut(
      List<Long> sourceTableIds, List<Long> backSetTableIds, CommitRecord commit) {
    
    if (!checkOutImpl(sourceTableIds, backSetTableIds, commit))
      return null;
    
    final List<Long> srcs = sourceTableIds, bset = backSetTableIds;
    return new Releaseable() {
      @Override
      public void close() {  release(srcs, bset);   }
    };
  }
  
  public boolean checkOutImpl(List<Long> sourceTableIds, List<Long> backSetTableIds, CommitRecord commit) {
    // sanity check the arguments
//    {
//      int expectedCount = sourceTableIds.size() + backSetTableIds.size();
//      Set<Long> uniqueIds = new HashSet<>(expectedCount);
//      uniqueIds.addAll(sourceTableIds);
//      uniqueIds.addAll(backSetTableIds);
//      if (uniqueIds.size() != expectedCount)
//        throw new IllegalArgumentException(
//            "arguments contain dup IDs: srcs=" + sourceTableIds + ", bset=" + backSetTableIds);
//    }
    synchronized (lock) {
      if (!advanceCommit(commit))
        return false;
      if (CollectionUtils.intersect(srcIds, sourceTableIds))
        return false;
      addRefs(sourceTableIds);
      addRefs(backSetTableIds);
      srcIds.addAll(sourceTableIds);
    }
    return true;
  }
  
  
  private void release(List<Long> sourceTableIds, List<Long> backSetTableIds) {
    synchronized (lock) {
      int preSize = srcIds.size();
      srcIds.removeAll(sourceTableIds);
      int postSize = srcIds.size();
      // sanity check
      if (postSize != preSize - sourceTableIds.size())
        throw new RuntimeException(
            "assertion failed: stids=" + sourceTableIds + "; this[modified by failure]=" + this);
      releaseRefs(sourceTableIds);
      releaseRefs(backSetTableIds);
    }
  }
  
  
  private void addRefs(Iterable<Long> tableIds) {
    for (Long tableId : tableIds)
      addRef(tableId);
  }
  
  private void addRef(Long tableId) {
    int[] refCountPtr = tableRefCounts.get(tableId);
    if (refCountPtr == null) {
      refCountPtr = new int[] { 1 };
      tableRefCounts.put(tableId, refCountPtr);
      lifecycleListener.inited(tableId);
    }
    else
      ++refCountPtr[0];
  }

  
  
  private void releaseRefs(Iterable<Long> tableIds) {
    for (Long tableId : tableIds)
      releaseRef(tableId);
  }
  
  private void releaseRef(Long tableId) {
    int[] refCountPtr = tableRefCounts.get(tableId);
    if (refCountPtr == null) {
      if (strict)
        throw new IllegalStateException(
            "attempt to decrement ref to null table " + tableId + "; this=" + this);
      return;
    }
    
    int refCount = --refCountPtr[0];
    
    if (refCount > 0)
      return;

    if (refCount == 0) {
      
      tableRefCounts.remove(tableId);
      lifecycleListener.released(tableId);
    
    } else if (strict) {
      throw new RuntimeException(
          "assertion failed. tableId=" + tableId + "; refCount=" + refCountPtr + "; this=" + this);
    }
  }
  
  

}
