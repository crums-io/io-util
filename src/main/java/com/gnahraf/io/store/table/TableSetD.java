/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table;


import java.io.IOException;
import java.nio.ByteBuffer;

import com.gnahraf.io.store.table.del.DeleteCodec;
import com.gnahraf.io.store.table.iter.TableSetDIterator;
import com.gnahraf.io.store.table.iter.TableSetIterator;
import com.gnahraf.io.store.table.order.RowOrder;

/**
 * A {@linkplain TableSet} supporting delete overrides.
 * 
 * @author Babak
 */
public class TableSetD extends TableSet {


  private final DeleteCodec deleteCodec;

  

  /**
   * Creates an empty instance.
   */
  public TableSetD(RowOrder order, int rowWidth, DeleteCodec deleteCodec) {
    super(order, rowWidth);
    this.deleteCodec = deleteCodec;
    if (deleteCodec == null)
      throw new IllegalArgumentException("null deleteCodec");
  }

  public TableSetD(SortedTable table, DeleteCodec deleteCodec) throws IOException {
    this(new SortedTable[]{ table }, deleteCodec, false);
    checkTable(table);
  }


  public TableSetD(SortedTable[] tables, DeleteCodec deleteCodec) throws IOException {
    this(tables, deleteCodec, true);
  }


  protected TableSetD(SortedTable[] tables, DeleteCodec deleteCodec, boolean checkAndClone) throws IOException {
    super(tables, checkAndClone);
    this.deleteCodec = deleteCodec;
    if (deleteCodec == null)
      throw new IllegalArgumentException("null deleteCodec");
  }


  @Override
  public ByteBuffer getRow(ByteBuffer key) throws IOException {
    ByteBuffer row = super.getRow(key);
    if (row != null && deleteCodec.isDeleted(row))
      row = null;
    return row;
  }
  

  @Override
  public TableSetIterator iterator() throws IOException {
    return new TableSetDIterator(this);
  }
  

  @Override
  public TableSetD append(SortedTable table) throws IOException {
    return new TableSetD(appendImpl(table), deleteCodec, false);
  }


  @Override
  public TableSetD append(SortedTable... table) throws IOException {
    return new TableSetD(appendImpl(table), deleteCodec, false);
  }


  public final DeleteCodec getDeleteCodec() {
    return deleteCodec;
  }
  
  

}
