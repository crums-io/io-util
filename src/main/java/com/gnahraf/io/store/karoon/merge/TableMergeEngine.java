/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon.merge;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.Channel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.gnahraf.io.Releaseable;
import com.gnahraf.io.store.karoon.CommitRecord;
import com.gnahraf.io.store.karoon.KaroonException;
import com.gnahraf.io.store.karoon.SidTable;
import com.gnahraf.io.store.karoon.TStore;
import com.gnahraf.io.store.karoon.TStore.TmeContext;
import com.gnahraf.io.store.table.TableSet;
import com.gnahraf.util.TaskStack;
import com.gnahraf.util.cc.FixedPriorityThreadFactory;
import com.gnahraf.util.cc.RunState;

/**
 * 
 * @author Babak
 */
public class TableMergeEngine implements Channel {
  
  private static class YoungMerge {
    final GenerationInfo g;
    final TableMerge tmerge;
    
    YoungMerge(GenerationInfo g, TableMerge tmerge) {
      this.g = g;
      this.tmerge = tmerge;
    }
  }
  
  private final static Logger LOG = Logger.getLogger(TableMergeEngine.class.getName());
  
  private final TmeContext storeContext;
  private final TStore tableStore;
  private final ExecutorService threadPool;
  
  
  private volatile boolean stopped;

  
  private final Object freshMeatLock = new Object();
  
  private final Map<Integer, TableMerge> activeMerges =
      Collections.synchronizedMap(new HashMap<Integer, TableMerge>());
  
  private final List<YoungMerge> youngActiveMerges =
      Collections.synchronizedList(new ArrayList<YoungMerge>());
  
  private final Object oldGenerationLock = new Object();
  
  // Doesn't belong here
//  private final TableRegistry tableRegistry;

  private final String gclLabel;
  private final String yclLabel;
  private final String gmLabel;
  private final String ymLabel;
  
  private RunState state = RunState.INIT;
  
  /**
   * @throws NullPointerException
   *         if <tt>storeContext</tt> is <tt>null</tt>
   */
  public TableMergeEngine(TmeContext storeContext) {
    this(storeContext, null);
  }
  
  /**
   * @throws NullPointerException
   *         if <tt>storeContext</tt> is <tt>null</tt>
   */
  public TableMergeEngine(TmeContext storeContext, ExecutorService threadPool) {
    this.storeContext = storeContext;
    this.tableStore = storeContext.store();
    if (!tableStore.isOpen())
      throw new IllegalArgumentException("closed tableStore: " + tableStore);
    
    if (threadPool == null) {
      ThreadFactory threadFactory =
          new FixedPriorityThreadFactory(
              storeContext.store().getConfig().getMergePolicy().getMergeThreadPriority());
      threadPool = Executors.newCachedThreadPool(threadFactory);
    } else if (threadPool.isShutdown())
      throw new IllegalArgumentException("threadPool is shutdown: " + threadPool);
    
    this.threadPool = threadPool;
    
//    TableLifecycleListener lifecycleListener = new TableLifecycleListener() {
//      @Override
//      public void released(long tableId) {
//        TableMergeEngine.this.storeContext.discardTable(tableId);
//      }
//      @Override
//      public void inited(long tableId) {
//      }
//    };
//    this.tableRegistry = new TableRegistry(lifecycleListener);
    
    this.gclLabel = this + " - Generational control loop: ";
    this.yclLabel = this + " - Young control loop: ";
    this.gmLabel = this + " - Generational merge: ";
    this.ymLabel = this + " - Young table merge: ";
  }
  
  public void notifyFreshMeat() {
    synchronized (freshMeatLock) {
      freshMeatLock.notify();
    }
  }
  
  public void start() {
    synchronized (this) {
      if (state.hasStarted())
        throw new IllegalStateException(this + ": already started");
      state = RunState.STARTED;
    }
    threadPool.execute(youngMergeLoop());
    try {
      Thread.sleep(100);
    } catch (InterruptedException ix) {
      LOG.severe(this + " interrupted: " + ix.getMessage() + " - stopping..");
      stop();
      return;
    }
    threadPool.execute(generationalMergeControlLoop());
    LOG.info(this + " [STARTED]");
  }
  
  
  private Runnable youngMergeLoop() {
    return new Runnable() {
      @Override
      public void run() { performYoungMerges(); }
    };
  }

  public boolean await(long millis) throws InterruptedException {
    return threadPool.awaitTermination(millis, TimeUnit.MILLISECONDS);
  }
  
  
  private void notifyOldGeneration() {
    synchronized (oldGenerationLock) {
      oldGenerationLock.notify();
    }
  }
  
  private CommitInfo getCommitInfo() throws IOException {
    // table file deletions are mediated through the table registry
    // (via the lifecycle listener set up in the constructor)
    // So we lock the registry so that files don't slip away underneath
    // us while we check their sizes..
    synchronized (storeContext.tableRegistry()) {
      CommitRecord commit = tableStore.getCurrentCommit();
      return CommitInfo.getCommitInfo(commit, tableStore);
    }
  }
  
  
  private void performYoungMerges() {
    LOG.info(yclLabel + "STARTED");
    LOG.info(yclLabel + "Thread ID = " + Thread.currentThread().getId());
    while (!stopped) {
      try {
        
        // If there are 2 concurrent merges already running, wait..
        if (youngActiveMerges.size() > 1) {
          synchronized (youngActiveMerges) {
            while (youngActiveMerges.size() > 1)
              youngActiveMerges.wait();
            
            // ( notified by newYoungMerge(..) Runnable or stop() )
          }
          continue;
        }
        
        CommitInfo commitInfo = getCommitInfo();
        GenerationInfo g = getYoungGenerationInfo(commitInfo);
        if (g == null) {
          synchronized (freshMeatLock) {
            while (!stopped) {
              
              commitInfo = getCommitInfo();
              g = getYoungGenerationInfo(commitInfo);
              if (g != null)
                break;
              
              LOG.fine(yclLabel + "waiting for fresh meat..");
              
              freshMeatLock.wait();
              
              LOG.fine(yclLabel + "notified");
            }
          }
          if (stopped)
            break;
        }
        // true: g != null
        
        synchronized (youngActiveMerges) {
          // if there's another concurrent zero generation merge running
          // reduce the job
          if (!youngActiveMerges.isEmpty()) {
            GenerationInfo priorGi = youngActiveMerges.get(0).g;
            g = g.reduceBy(priorGi);
          }
          
          // if, as a result of the reduction, we got nothing to merge..
          if (g == null)
            continue;
          
          Runnable mergeTask = newYoungMerge(g, commitInfo);
          if (mergeTask != null)
            threadPool.execute(mergeTask);
        }
        
        
      } catch (Exception x) {
        LOG.severe(yclLabel + "aborting on error. Detail: " + x.getMessage());
        break;
      }
    }
    if (!stopped) {
      LOG.warning(yclLabel + "stopping merge engine on abort");
      stop();
    }
    LOG.info(yclLabel + "STOPPED");
  }
  
  
  
  private GenerationInfo getYoungGenerationInfo(CommitInfo commitInfo) throws IOException {
    MergePolicy mergePolicy = tableStore.getConfig().getMergePolicy();
    return GenerationInfo.candidateMerge(commitInfo.tableInfos(), mergePolicy, 0);
  }
  
  
  
  private final static Comparator<GenerationInfo> MERGE_BANG_4_BUCK_RANK =
      new Comparator<GenerationInfo>() {
        @Override
        public int compare(GenerationInfo a, GenerationInfo b) {
          double ra = a.effectToBytesRatio();
          double rb = b.effectToBytesRatio();
          if (ra > rb)
            return -1;
          else if (rb > ra)
            return 1;
          else
            return 0;
        }
      };
  
  protected Runnable generationalMergeControlLoop() {
    return new Runnable() {
      @Override
      public void run() {
        
        LOG.info(gclLabel + "STARTED");
        LOG.info(gclLabel + "Thread ID = " + Thread.currentThread().getId());
        
        while (!stopped) {
          TaskStack closer = new TaskStack();
          try {

            final CommitInfo commitInfo;
            List<GenerationInfo> mergeCandidates;
            
            synchronized (oldGenerationLock) {
              // wait if there are already too many merges in progress..
              while (!stopped && mergeThreadsSaturated())
                oldGenerationLock.wait();
              if (stopped)
                break;
              
              commitInfo = getCommitInfo();
              
              MergePolicy mergePolicy = tableStore.getConfig().getMergePolicy();
              mergeCandidates =
                  GenerationInfo.generationalMergeCandidates(
                      commitInfo.tableInfos(),
                      mergePolicy);
              
              // wait if there are no merge candidates
              if (mergeCandidates.isEmpty()) {
                oldGenerationLock.wait();
                continue;
              }
            }
            
            
            
            Collections.sort(mergeCandidates, MERGE_BANG_4_BUCK_RANK);
            
            for (
                Iterator<GenerationInfo> ig = mergeCandidates.iterator();
                ig.hasNext() && !stopped && !mergeThreadsSaturated(); )
            {
              GenerationInfo g = ig.next();
              if (activeMerges.containsKey(g.generation))
                continue;
              try {
                
                Runnable mergeOp = newGenerationMerge(g, commitInfo);
                if (mergeOp != null) {
                  threadPool.execute(mergeOp);
                }
              } catch (FileNotFoundException fnfx) {
                // This is a an expected race between the control loop and the merge threads.
                // We can make the race go away with some careful synchronization, but
                // honestly, it's not worth it. Taking the optimistic approach wherever possible
                
                // sanity check nothing really amiss..
                List<Long> committedTableIds = tableStore.getCurrentCommit().getTableIds();
                if (committedTableIds.containsAll(g.srcIds()) &&
                    committedTableIds.containsAll(g.backSetIds()) ) {
                  throw new KaroonException(
                      "Assertion failure. g=" + g + "; cids=" + committedTableIds +
                      ". Cascading on FNF error: " + fnfx.getMessage(), fnfx);
                }

                LOG.warning(gclLabel + "Skipping already merged " + g + " - expected low frequency event");
              }
            }
            
          } catch (InterruptedException ix) {
            if (stopped)
              break;
          } catch (Exception x) {
            LOG.severe(gclLabel + "aborting on error. Detail: " + x.getMessage());
            break;
          } finally {
            closer.close();
          }
        }
        
        if (!stopped) {
          LOG.warning(gclLabel + "stopping merge engine on abort");
          stop();
        }
        LOG.info(gclLabel + "STOPPED");
      }
    };
  }
  
  
  private boolean mergeThreadsSaturated() {
    int maxThreads = tableStore.getConfig().getMergePolicy().getMaxMergeThreads();
    return stopped || activeMerges.size() - 1 >= maxThreads;
  }
  
  
  protected Runnable newYoungMerge(GenerationInfo g, CommitInfo commitInfo) throws IOException {
    
    final Releaseable checkout = storeContext.tableRegistry().checkOut(
        g.srcIds(),
        g.backSetIds(),
        commitInfo.commitRecord());
    if (checkout == null)
      return null;
      
    final TableMerge tmerge = prepareGenerationalMerge(g);
    final YoungMerge youngMerge = new YoungMerge(g, tmerge);
    youngActiveMerges.add(youngMerge);
    return new Runnable() {
      @Override
      public void run() {

        boolean panick = false;
        tmerge.run();
        
        try {
          
          if (tmerge.getState().succeeded()) {
            storeContext.tablesMerged(tmerge.getSourceIds(), tmerge.getOutTable());
            storeContext.tableRegistry().advanceCommit(tableStore.getCurrentCommit());
            if (tmerge.getOutputFile().length() > tableStore.getConfig().getMergePolicy().getMaxYoungSize())
              notifyOldGeneration();
          } else if (stopped) {
            LOG.info(ymLabel + "aborted on stop");
            if (tmerge.getOutTable() != null)
              tmerge.getOutTable().close();
          }
          else {
            // shouldn't ever get here
            LOG.severe(ymLabel + "[FAILED] - " + tmerge.getException());
            panick = true;
          }
          
        } catch (Exception x) {
          LOG.severe(ymLabel + "[FAILED]. Detail: " + x.getMessage());
          panick = true;
        } finally {
          tmerge.close();
          checkout.close();
          if (panick) {
            LOG.severe("Stopping..");
            stop();
          } else {
            synchronized (youngActiveMerges) {
              youngActiveMerges.remove(youngMerge);
              youngActiveMerges.notifyAll();
            }
          }
        }
      }
    };
  }
  
  protected Runnable newGenerationMerge(GenerationInfo g, CommitInfo commitInfo) throws IOException {
    final Releaseable checkout = storeContext.tableRegistry().checkOut(
        g.srcIds(),
        g.backSetIds(),
        commitInfo.commitRecord());
    
    if (checkout == null)
      return null;
    final TableMerge merge = prepareGenerationalMerge(g);
    final int generation = g.generation;
    activeMerges.put(generation, merge);
    return new Runnable() {
      @Override
      public void run() {
        
        boolean panick = false;
        merge.run();
        
        try {
          
          if (merge.getState().succeeded()) {
            storeContext.tablesMerged(merge.getSourceIds(), merge.getOutTable());
            storeContext.tableRegistry().advanceCommit(tableStore.getCurrentCommit());
          } else if (stopped)
            LOG.info(gmLabel + "aborted generation " + generation + " on stop");
          else {
            // shouldn't ever get here
            LOG.severe(gmLabel + "[FAILED] - " + merge.getException());
            panick = true;
          }
        
        } catch (Exception x) {
          LOG.severe(gmLabel + "[FAILED]. Detail: " + x.getMessage());
          panick = true;
        } finally {
          merge.close();
          removeFromActiveMerges(generation);
          checkout.close();
          if (panick) {
            LOG.severe("Stopping..");
            stop();
          }
        }
      }
    };
  }
  
  private void removeFromActiveMerges(int generation) {
    activeMerges.remove(generation);
    notifyOldGeneration();
  }
  
  public void stop() {
    synchronized (this) {
      if (stopped)
        return;
      stopped = true;
    }
    synchronized (activeMerges) {
      for (TableMerge merge : activeMerges.values()) {
        try {
          merge.abort();
        } catch (Exception x) {
          LOG.fine("On active merge abort: " + merge + " -- " + x);
        }
      }
    }
    
    synchronized (youngActiveMerges) {
      for (YoungMerge ymerge : youngActiveMerges) {
        try {
          ymerge.tmerge.abort();
        } catch (Exception x) {
          LOG.fine("On young active merge abort: " + ymerge.tmerge + " -- " + x);
        }
      }
    }
    notifyFreshMeat();
    notifyOldGeneration();
    threadPool.shutdown();
    storeContext.store().close();
  }
  
  
  
  public void stopImmediately() {
    stop();
    threadPool.shutdownNow();
  }
//  
//  private TableMerge prepareYoungMerge() throws IOException {
//    List<Long> tableIds = tableStore.getCurrentCommit().getTableIds();
//    MergePolicy mergePolicy = tableStore.getConfig().getMergePolicy();
//    
//    if (tableIds.size() < mergePolicy.getMinYoungMergeTableCount())
//      return null;
//    
//    final int youngCount;
//    {
//      int count = 0;
//      for (int i = tableIds.size(); i-- > 0; ) {
//        long size = tableStore.getTableFileSize(tableIds.get(i));
//        if (size <= mergePolicy.getMaxYoungSize())
//          ++count;
//        else
//          break;
//      }
//      youngCount = count;
//    }
//    
//    if (youngCount < mergePolicy.getMinYoungMergeTableCount() || youngCount < 2)
//      return null;
//    
//
//    final int backSetTableCount = tableIds.size() - youngCount;
//    
//    TaskStack closerOnFail = new TaskStack();
//    boolean failed = true;
//    try {
//      TableSet backSet;
//      if (backSetTableCount == 0) {
//        backSet = null;
//      } else {
//        SidTable[] backTables = new SidTable[backSetTableCount];
//        for (int i = 0; i < backSetTableCount; ++i) {
//          backTables[i] = tableStore.loadSortedTable(tableIds.get(i));
//          closerOnFail.pushClose(backTables[i]);
//        }
//        backSet = new TableSet(backTables);
//      }
//      
//      SidTable[] sources = new SidTable[youngCount];
//      for (int i = 0; i < youngCount; ++i) {
//        sources[i] = tableStore.loadSortedTable(tableIds.get(i + backSetTableCount));
//        closerOnFail.pushClose(sources[i]);
//      }
//      
//      long mergedTableId = storeContext.newTableId();
//      File mergedTableFile = storeContext.newTablePath(mergedTableId);
//      
//      TableMerge merge = new TableMerge(
//          sources,
//          tableStore.getConfig().getDeleteCodec(),
//          backSet,
//          mergedTableFile,
//          mergedTableId);
//      
//      failed = false;
//      return merge;
//      
//    } finally {
//      if (failed)
//        closerOnFail.close();
//    }
//  }
//  

  
  private TableMerge prepareGenerationalMerge(GenerationInfo g) throws IOException {
    TaskStack closerOnFail = new TaskStack();
    boolean failed = true;
    try {
      SidTable[] sources = loadTableStack(g.srcInfos, closerOnFail);
      TableSet backSet;
      if (g.backSetInfos.isEmpty())
        backSet = null;
      else {
        SidTable[] backTables = loadTableStack(g.backSetInfos, closerOnFail);
        backSet = new TableSet(backTables);
      }

      long mergedTableId = storeContext.newTableId();
      File mergedTableFile = storeContext.newTablePath(mergedTableId);
       
      TableMerge merge = new TableMerge(
          g,
          sources,
          tableStore.getConfig().getDeleteCodec(),
          backSet,
          mergedTableFile,
          mergedTableId);;
      
      failed = false;
      return merge;
      
    } finally {
      if (failed)
        closerOnFail.close();
    }
  }
  
  
  private SidTable[] loadTableStack(List<TableInfo> infos, TaskStack closerOnFail) throws IOException {
    SidTable[] tables = new SidTable[infos.size()];
    for (int i = 0; i < tables.length; ++i) {
      tables[i] = tableStore.loadSortedTable( infos.get(i).tableId );
      closerOnFail.pushClose(tables[i]);
    }
    return tables;
  }

  @Override
  public boolean isOpen() {
    return !stopped;
  }

  @Override
  public void close() {
    stop();
  }
  
  public String toString() {
    return "[" + tableStore.getConfig().getRootDir().getName() + "]";
  }

}
