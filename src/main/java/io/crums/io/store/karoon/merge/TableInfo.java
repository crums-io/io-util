/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.karoon.merge;


/**
 * A table's ID and byte size. Used for deciding which tables to merge.
 * 
 * @author Babak
 */
public final class TableInfo {
  
  public final long tableId;
  public final long size;
  
  /**
   * For package-private invocation by {@linkplain TableMergeEngine}.
   * Arguments are not checked.
   */
  TableInfo(long tableId, long size) {
    this.tableId = tableId;
    this.size = size;
  }
  
  @Override
  public String toString() {
    return "[id=" + tableId + ", sz=" + size + "]";
  }
  

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    else if (o instanceof TableInfo) {
      TableInfo other = (TableInfo) o;
      return tableId == other.tableId && other.size == size;
    } else
      return false;
  }
  

  @Override
  public int hashCode() {
    long lh = tableId ^ size;
    return (int) (lh ^ (lh >>> 32));
  }
  
  
}