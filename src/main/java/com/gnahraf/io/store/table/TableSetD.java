/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.gnahraf.io.store.table.del.DeleteCodec;

/**
 * A {@linkplain TableSet} supporting delete overrides.
 * 
 * @author Babak
 */
public class TableSetD extends TableSet {


  private final DeleteCodec deleteCodec;


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
  public TableSetD append(SortedTable table) throws IOException {
    return new TableSetD(appendImpl(table), deleteCodec, false);
  }


  @Override
  public TableSetD append(SortedTable... table) throws IOException {
    return new TableSetD(appendImpl(table), deleteCodec, false);
  }
  
  

}
