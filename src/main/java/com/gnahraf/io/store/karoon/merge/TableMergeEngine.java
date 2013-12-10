/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon.merge;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.gnahraf.io.store.karoon.CommitRecord;
import com.gnahraf.io.store.karoon.TStore;

/**
 * 
 * @author Babak
 */
public class TableMergeEngine {
  
  private final static Logger LOG = Logger.getLogger(TableMergeEngine.class);
  
  private final TStore tableStore;
  
  public TableMergeEngine(TStore tableStore) {
    this.tableStore = tableStore;
    if (tableStore == null)
      throw new IllegalArgumentException("null tableStore");
    if (!tableStore.isOpen())
      throw new IllegalArgumentException("closed tableStore: " + tableStore);
  }
  
  
  public void mergeYoung() throws IOException {
    CommitRecord commitRecord = tableStore.getCurrentCommit();
    List<Long> tableIds = commitRecord.getTableIds();
    MergePolicy mergePolicy = tableStore.getConfig().getMergePolicy();
    if (tableIds.size() < mergePolicy.getYoungThreshold())
      return;
    
    int youngCount = 0;
    for (int i = 0; i < tableIds.size(); ++i) {
      long size = tableStore.getTableFileSize(tableIds.get(i));
      if (size <= mergePolicy.getMaxYoungSize())
        ++youngCount;
      else
        break;
    }
    
    if (youngCount  < mergePolicy.getYoungThreshold())
      return;
  }

}
