/*
 * Copyright 2014 Babak Farhang 
 */
package io.crums.io.store.karoon.merge;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.crums.io.store.karoon.CommitRecord;
import io.crums.io.store.karoon.TStore;

/**
 * Encapsulates table file sizes for a given commit.
 * 
 * @author Babak
 */
public class CommitInfo {
  
  
  public static CommitInfo getCommitInfo(CommitRecord commit, TStore store) throws IOException {
    List<TableInfo> tableInfos = new ArrayList<>(commit.getTableIds().size());
    for (Long tid : commit.getTableIds()) {
      long tsize = store.getTableFileSize(tid);
      tableInfos.add(new TableInfo(tid, tsize));
    }
    tableInfos = Collections.unmodifiableList(tableInfos);
    return new CommitInfo(commit, tableInfos);
  }
  
  
  
  
  
  
  
  
  private final CommitRecord commit;
  private final List<TableInfo> tableInfos;
  
  
  private CommitInfo(CommitRecord commit, List<TableInfo> tableInfos) {
    this.commit = commit;
    this.tableInfos = tableInfos;
  }
  
  
  public final CommitRecord commitRecord() {
    return commit;
  }
  
  
  public final List<TableInfo> tableInfos() {
    return tableInfos;
  }

}
