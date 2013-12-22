/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon.merge;


import java.io.File;
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
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.gnahraf.io.store.karoon.KaroonException;
import com.gnahraf.io.store.karoon.SidTable;
import com.gnahraf.io.store.karoon.TStore;
import com.gnahraf.io.store.karoon.TStore.TmeContext;
import com.gnahraf.io.store.table.TableSet;
import com.gnahraf.util.TaskStack;
import com.gnahraf.util.cc.RunState;

/**
 * 
 * @author Babak
 */
public class TableMergeEngine implements Channel {
  
  private final static Logger LOG = Logger.getLogger(TableMergeEngine.class);
  
  private final TmeContext storeContext;
  private final TStore tableStore;
  private final ExecutorService threadPool;
  
  
  private volatile boolean stopped;

  
  private final Object freshMeatLock = new Object();
  
  private final Map<Integer, TableMerge> activeMerges =
      Collections.synchronizedMap(new HashMap<Integer, TableMerge>());
  
  private final Object oldGenerationLock = new Object();
  

  private final String gclLabel;
  private final String gmLabel;
  private final String ymlLabel;
  
  private RunState state = RunState.INIT;
  
  /**
   * @throws NullPointerException
   */
  public TableMergeEngine(TmeContext storeContext) {
    this.storeContext = storeContext;
    this.tableStore = storeContext.store();
    if (!tableStore.isOpen())
      throw new IllegalArgumentException("closed tableStore: " + tableStore);
    this.threadPool = Executors.newCachedThreadPool();
    this.gclLabel = this + " - Generational control loop: ";
    this.gmLabel = this + " - Generational merge: ";
    this.ymlLabel = this + " - Young table merge loop: ";
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
      LOG.error(this + " interrupted: " + ix.getMessage() + " - stopping..");
      stop();
      return;
    }
    threadPool.execute(generationalMergeControlLoop());
    LOG.info(this + " [STARTED]");
  }
  
  
  public boolean await(long millis) throws InterruptedException {
    return threadPool.awaitTermination(millis, TimeUnit.MILLISECONDS);
  }
  
  
  private void notifyOldGeneration() {
    synchronized (oldGenerationLock) {
      oldGenerationLock.notify();
    }
  }
  
  private List<TableInfo> getInfoSnapshot() throws IOException {
    List<Long> tableIds = tableStore.getCurrentCommit().getTableIds();
    ArrayList<TableInfo> tfs = new ArrayList<>(tableIds.size());
    for (Long tableId : tableIds) {
      long size = tableStore.getTableFileSize(tableId);
      tfs.add(new TableInfo(tableId, size));
    }
    return Collections.unmodifiableList(tfs);
  }
  
  protected Runnable youngMergeLoop() {
    return new Runnable() {
      @Override
      public void run() {
        LOG.info(ymlLabel + "STARTED");
        LOG.info(ymlLabel + "Thread ID = " + Thread.currentThread().getId());
        while (!stopped) {
          TaskStack closer = new TaskStack();
          try {
            TableMerge merge = prepareYoungMerge();
            if (merge == null) {
              synchronized (freshMeatLock) {
                while ((merge = prepareYoungMerge()) == null && !stopped) {
                  LOG.debug(ymlLabel + "waiting for fresh meat..");
                  freshMeatLock.wait();
                  LOG.debug(ymlLabel + "notified");
                }
              }
              if (stopped)
                break;
            }
            closer.pushClose(merge);
            closer.pushRun(removeFromActiveMergesOp(0));
            
            LOG.debug(ymlLabel + "merge prepared.");
            activeMerges.put(0, merge);
            merge.run();
            
            if (stopped) {
              // we have to check for this corner case..
              if (merge.getOutTable() != null)
                closer.pushClose(merge.getOutTable());
              break;
            }
            
            if (!merge.getState().succeeded()) {
              if (merge.getException() != null)
                throw merge.getException();
              else {
                // should never end up here
                if (merge.getOutTable() != null)
                  closer.pushClose(merge.getOutTable());
                throw new KaroonException("Unspecified failure with merge " + merge);
              }
            }
            
            storeContext.tablesMerged(merge.getSourceIds(), merge.getOutTable());
            if (merge.getOutputFile().length() > tableStore.getConfig().getMergePolicy().getMaxYoungSize())
              notifyOldGeneration();
            
          } catch (Exception x) {
            LOG.error(ymlLabel + "aborting on error. Detail: " + x.getMessage(), x);
            break;
          } finally {
            closer.close();
          }
        }
        if (!stopped) {
          LOG.warn(ymlLabel + "stopping merge engine on abort");
          stop();
        }
        LOG.info(ymlLabel + "STOPPED");
      }
    };
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
            
            List<GenerationInfo> mergeCandidates;
            
            synchronized (oldGenerationLock) {
              // wait if there are already too many merges in progress..
              while (!stopped && mergeThreadsSaturated())
                oldGenerationLock.wait();
              if (stopped)
                break;
              
              List<TableInfo> tableStack = getInfoSnapshot();
              MergePolicy mergePolicy = tableStore.getConfig().getMergePolicy();
              mergeCandidates =
                  GenerationInfo.generationalMergeCandidates(tableStack, mergePolicy);
              
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
              Runnable mergeOp = newGenerationMerge(ig.next());
              threadPool.execute(mergeOp);
            }
            
          } catch (InterruptedException ix) {
            if (stopped)
              break;
          } catch (Exception x) {
            LOG.error(gclLabel + "aborting on error. Detail: " + x.getMessage(), x);
            break;
          } finally {
            closer.close();
          }
        }
        
        if (!stopped) {
          LOG.warn(gclLabel + "stopping merge engine on abort");
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
  
  
  protected Runnable newGenerationMerge(GenerationInfo g) throws IOException {
    final TableMerge merge = prepareGenerationalMerge(g);
    final int generation = g.generation;
    activeMerges.put(generation, merge);
    return new Runnable() {
      @Override
      public void run() {
        
        boolean panick = false;
        merge.run();
        
        try {
          
          if (merge.getState().succeeded())
            storeContext.tablesMerged(merge.getSourceIds(), merge.getOutTable());
          else if (stopped)
            LOG.info(gmLabel + "aborted generation " + generation + " on stop");
          else {
            // shouldn't ever get here
            LOG.error(gmLabel + "[FAILED] - " + merge.getException());
            panick = true;
          }
        
        } catch (Exception x) {
          LOG.error(gmLabel + "[FAILED]. Detail: " + x.getMessage(), x);
          panick = true;
        } finally {
          merge.close();
          removeFromActiveMerges(generation);
          if (panick) {
            LOG.error("Stopping..");
            stop();
          }
        }
      }
    };
  }
  
  private Runnable removeFromActiveMergesOp(final int generation) {
    return new Runnable() {
      @Override
      public void run() {
        removeFromActiveMerges(generation);
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
        } catch (Exception x) {  }
      }
    }
    notifyFreshMeat();
    notifyOldGeneration();
    threadPool.shutdown();
  }
  
  
  public void stopImmediately() {
    stop();
    threadPool.shutdownNow();
  }
  
  private TableMerge prepareYoungMerge() throws IOException {
    List<Long> tableIds = tableStore.getCurrentCommit().getTableIds();
    MergePolicy mergePolicy = tableStore.getConfig().getMergePolicy();
    
    if (tableIds.size() < mergePolicy.getMinYoungMergeTableCount())
      return null;
    
    final int youngCount;
    {
      int count = 0;
      for (int i = tableIds.size(); i-- > 0; ) {
        long size = tableStore.getTableFileSize(tableIds.get(i));
        if (size <= mergePolicy.getMaxYoungSize())
          ++count;
        else
          break;
      }
      youngCount = count;
    }
    
    if (youngCount < mergePolicy.getMinYoungMergeTableCount() || youngCount < 2)
      return null;
    

    final int backSetTableCount = tableIds.size() - youngCount;
    
    TaskStack closerOnFail = new TaskStack();
    boolean failed = true;
    try {
      TableSet backSet;
      if (backSetTableCount == 0) {
        backSet = null;
      } else {
        SidTable[] backTables = new SidTable[backSetTableCount];
        for (int i = 0; i < backSetTableCount; ++i) {
          backTables[i] = tableStore.loadSortedTable(tableIds.get(i));
          closerOnFail.pushClose(backTables[i]);
        }
        backSet = new TableSet(backTables);
      }
      
      SidTable[] sources = new SidTable[youngCount];
      for (int i = 0; i < youngCount; ++i) {
        sources[i] = tableStore.loadSortedTable(tableIds.get(i + backSetTableCount));
        closerOnFail.pushClose(sources[i]);
      }
      
      long mergedTableId = storeContext.newTableId();
      File mergedTableFile = storeContext.newTablePath(mergedTableId);
      
      TableMerge merge = new TableMerge(
          sources,
          tableStore.getConfig().getDeleteCodec(),
          backSet,
          mergedTableFile,
          mergedTableId);
      
      failed = false;
      return merge;
      
    } finally {
      if (failed)
        closerOnFail.close();
    }
  }
  

  
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
