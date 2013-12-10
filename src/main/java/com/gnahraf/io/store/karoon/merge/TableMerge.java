/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon.merge;


import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;

import org.apache.log4j.Logger;

import com.gnahraf.io.store.karoon.SidTable;
import com.gnahraf.io.store.table.SortedTable;
import com.gnahraf.io.store.table.TableSet;
import com.gnahraf.io.store.table.del.DeleteCodec;
import com.gnahraf.io.store.table.merge.SetMergeSortD;
import com.gnahraf.io.store.table.order.RowOrder;
import com.gnahraf.util.cc.RunState;

/**
 * A <tt>SidTable</tt> merge operation.
 * 
 * @author Babak
 */
public class TableMerge implements Runnable {
  
  private final static Logger LOG = Logger.getLogger(TableMerge.class);
  
  private final List<SidTable> sources;
  private final DeleteCodec deleteCodec;
  private final TableSet backSet;
  private final File outputFile;
  private final long outTableId;
  private SidTable outTable;
  private SetMergeSortD sorter;

  private RunState state = RunState.INIT;
  private Exception x;
  
  
  TableMerge(
      List<SidTable> sources,
      DeleteCodec deleteCodec,
      TableSet backSet,
      File outputFile,
      long outTableId) {
    
    this.sources = sources;
    this.deleteCodec = deleteCodec;
    this.backSet = backSet;
    this.outputFile = outputFile;
    this.outTableId = outTableId;
  }
  

  @Override
  public void run() {
    state = RunState.STARTED;
    LOG.info(this);
    boolean failed = true;
    FileChannel out = null;
    try {
      out = new RandomAccessFile(outputFile, "rw").getChannel();
      outTable = new SidTable(out, getRowWidth(), getRowOrder(), outTableId);
      sorter = new SetMergeSortD(
          outTable,
          sources.toArray(new SortedTable[sources.size()]),
          deleteCodec,
          backSet);
      
      sorter.mergeToTarget();
      
      failed = false;
    } catch (Exception x) {
      this.x = x;
      LOG.error(this, x);
    } finally {
        state = failed ? RunState.FAILED : RunState.SUCCEEDED;
        LOG.info(this);
        
        // clean up if we had a messy failure..
        if (outTable == null && out != null) try {
          out.close();
        } catch (Exception x) {
          LOG.error("cascaded exception on out.close(): " + x.getMessage(), x);
        }
    }
  }
  
  public int getRowWidth() {
    return sources.get(0).getRowWidth();
  }
  
  public RowOrder getRowOrder() {
    return sources.get(0).order();
  }

  
  public final long getStartTime() {
    return getSorter().getStartTime();
  }


  private SetMergeSortD getSorter() {
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
    return sources;
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


  public String toString() {
    return
        "[srcs=" + sources +
        ", bset=" + backSet +
        ", out=" + outputFile.getName() +
        ", state=" + state + "]";
  }

}
