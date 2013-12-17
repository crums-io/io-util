/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon.merge;


import java.io.Closeable;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.gnahraf.io.store.karoon.SidTable;
import com.gnahraf.io.store.table.TableSet;
import com.gnahraf.io.store.table.del.DeleteCodec;
import com.gnahraf.io.store.table.merge.SetMergeSortD;
import com.gnahraf.io.store.table.order.RowOrder;
import com.gnahraf.util.CollectionUtils;
import com.gnahraf.util.TaskStack;
import com.gnahraf.util.cc.RunState;

/**
 * A <tt>SidTable</tt> merge operation.
 * 
 * @author Babak
 */
public class TableMerge implements Runnable, Closeable {
  
  private final static Logger LOG = Logger.getLogger(TableMerge.class);
  
  private final SidTable[] sources;
  private final DeleteCodec deleteCodec;
  private final TableSet backSet;
  private final File outputFile;
  private final long outTableId;
  private final TaskStack closer;
  private SidTable outTable;
  private SetMergeSortD sorter;

  private RunState state = RunState.INIT;
  private Exception x;
  
  
  TableMerge(
      SidTable[] sources,
      DeleteCodec deleteCodec,
      TableSet backSet,
      File outputFile,
      long outTableId) {
    
    this.sources = sources;
    this.deleteCodec = deleteCodec;
    this.backSet = backSet;
    this.outputFile = outputFile;
    this.outTableId = outTableId;
    this.closer = new TaskStack();
    closer.pushClose(sources);
    if (backSet != null)
      closer.pushClose(backSet);
  }
  
  
  /**
   * Closes all the open (I/O) resoures associated with this merge, excepting
   * the {@linkplain #getOutTable() out table}.
   */
  @Override
  public void close() {
    closer.close();
  }
  

  @Override
  public void run() {
    if (state.hasStarted())
      throw new IllegalStateException("already started: " + this);
    state = RunState.STARTED;
    LOG.info(this);
    boolean failed = true;
    FileChannel out = null;
    try {
      out = new RandomAccessFile(outputFile, "rw").getChannel();
      outTable = new SidTable(out, getRowWidth(), getRowOrder(), outTableId);
      sorter = new SetMergeSortD(
          outTable,
          sources,
          deleteCodec,
          backSet);
      
      sorter.mergeToTarget();
      
      failed = false;
    } catch (Exception x) {
      this.x = x;
      LOG.error(this, x);
      if (out != null)
        closer.pushClose(out);
    } finally {
        state = failed || sorter.isAborted() ? RunState.FAILED : RunState.SUCCEEDED;
        LOG.info(this);
    }
  }
  
  
  public boolean abort() {
    return sorter != null && sorter.abort();
  }
  
  public int getRowWidth() {
    return sources[0].getRowWidth();
  }
  
  public RowOrder getRowOrder() {
    return sources[0].order();
  }

  
  public final long getStartTime() {
    return getSorter().getStartTime();
  }


  public SetMergeSortD getSorter() {
    if (sorter == null)
      throw new IllegalStateException("not started: " + this);
    return sorter;
  }


  public final long getEndTime() {
    return getSorter().getEndTime();
  }


  public final long getTimeTaken() {
    return getSorter().getTimeTaken();
  }
  
  public List<SidTable> getSources() {
    return CollectionUtils.asReadOnlyList(sources);
  }


  public DeleteCodec getDeleteCodec() {
    return deleteCodec;
  }


  public TableSet getBackSet() {
    return backSet;
  }


  public File getOutputFile() {
    return outputFile;
  }


  public long getOutTableId() {
    return outTableId;
  }


  public SidTable getOutTable() {
    return outTable;
  }


  public RunState getState() {
    return state;
  }


  public Exception getException() {
    return x;
  }
  
  
  public List<Long> getSourceIds() {
    List<Long> srcIds = new ArrayList<>(getSources().size());
    for (SidTable table : getSources())
      srcIds.add(table.id());
    return srcIds;
  }


  public String toString() {
    return
        "[srcs=" + Arrays.asList(sources) +
        ", bset=" + backSet +
        ", out=" + outputFile.getName() +
        ", state=" + state + "]";
  }

}
