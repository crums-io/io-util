/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.merge;


import java.io.IOException;
import java.nio.ByteBuffer;

import com.gnahraf.io.store.table.SortedTable;
import com.gnahraf.io.store.table.SortedTable.Searcher;

/**
 * We maintain a stack of these during a merge. Each instance encapsulates progress
 * from a single table. These are mutually comparable in a way that allows
 * us to easily track rows from which source table must be copied to the
 * target of a merge.
 * <p/>
 * <h4>Note</h4>
 * Don't be daunted by the self-referential type parameter <em><tt>S</tt></em>: it just
 * says that derived instances are mutually comparable.
 * 
 * @author Babak
 */
public abstract class BaseMergeSource<S extends BaseMergeSource> implements Comparable<S> {


  private final Searcher searcher;
  private final long rowCount;
  private final ByteBuffer row;
  private long rowCursor;
  
  public BaseMergeSource(SortedTable.Searcher searcher) throws IOException {
    if (searcher == null)
      throw new IllegalArgumentException("null searcher");
    this.searcher = searcher;
    this.rowCount = searcher.getTable().getRowCount();
    if (rowCount == 0)
      throw new IllegalArgumentException("empty table: " + searcher.getTable());

    this.row = ByteBuffer.allocate(searcher.getTable().getRowWidth());
    setRow(0);
  }
  
  
  public final Searcher searcher() {
    return searcher;
  }
  
  public final SortedTable table() {
    return searcher.getTable();
  }
  
  /**
   * Returns the row number of the current {@linkplain #row()}.
   */
  public long rowNumber() {
    return rowCursor;
  }
  
  /**
   * Sets the {@linkplain #rowNumber()} and loads the corresponding {@linkplain #row()};
   * unless the <tt>rowNum</tt> argument is equal to {@linkplain #getRowCount()}.
   * 
   * @throws IndexOutOfBoundsException
   */
  public void setRow(long rowNum) throws IOException {
    if (rowNum >= rowCount) {
      if (rowNum == rowCount) {
        rowCursor = rowCount;
        return;
      }
      throw new IndexOutOfBoundsException("rowNum/rowCount: " + rowNum + "/" + rowCount);
    }
    row.clear();
    searcher.getTable().read(rowNum, row);
    row.flip();
    rowCursor = rowNum;
  }
  
  /**
   * Returns the snapshot row count. (If there are concurrent additions to the
   * underlying table, those will be ignored.)
   * @return
   */
  public long rowCount() {
    return rowCount;
  }

  
  /**
   * Returns the contents of the current {@linkplain #rowNumber()}, if the
   * instance is not {@linkplain #finished()}; otherwise, the return value is undefined.
   * @return
   */
  public final ByteBuffer row() {
    return row;
  }
  
  /**
   * An instance is finished when all its rows have been copied to the target. I.e. when
   * <tt>{@linkplain #rowNumber()} == {@linkplain #rowCount()}.
   */
  public final boolean finished() {
    return rowCursor == rowCount;
  }
  
  
  
  
  

  @Override
  public String toString() {
    StringBuilder string = new StringBuilder(128);
    string.append(getClass().getSimpleName()).append('[');
    appendToStringFields(string).append(']');
    return string.toString();
  }
  
  
  protected StringBuilder appendToStringFields(StringBuilder string) {
    return string.append("table=").append(searcher.getTable()).append(", row=").append(row)
    .append(", rowCount=").append(rowCount).append(", rowNumber=").append(rowNumber());
  }
  
  
  
  protected int compareToImpl(BaseMergeSource other) {
    if (this.finished()) {
      return other.finished() ? 0 : -1;
    } else if (other.finished())
      return this.finished() ? 0 : 1;
    else
      return -table().order().compare(this.row, other.row);
  }

}
