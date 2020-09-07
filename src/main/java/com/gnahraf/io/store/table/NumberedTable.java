/*
 * Copyright 2020 Babak Farhang
 */
package com.gnahraf.io.store.table;

import java.io.IOException;
import java.nio.channels.FileChannel;

import com.gnahraf.io.store.table.order.RowOrder;

/**
 * A sorted table with numeric id.
 */
public class NumberedTable extends SortedTable {
  
  private final int id;

  /**
   * 
   * @param id the table's ID
   * 
   * @see SortedTable#SortedTable(FileChannel, int, RowOrder)
   */
  public NumberedTable(FileChannel file, int rowSize, RowOrder order, int id) throws IOException {
    super(file, rowSize, order);
    this.id = id;
  }

  /**
   * 
   * @param id the table's ID
   * 
   * @see SortedTable#SortedTable(FileChannel, long, int, RowOrder)
   */
  public NumberedTable(
      FileChannel file, long zeroRowFileOffset, int rowSize, RowOrder order, int id) throws IOException {
    super(file, zeroRowFileOffset, rowSize, order);
    this.id = id;
  }

  

  /**
   * Reference-counted copy constructor.
   * 
   * @see SortedTable#SortedTable(SortedTable)
   * @see #clone()
   */
  public NumberedTable(NumberedTable copy) {
    super(copy);
    this.id = copy.id;
  }
  
  
  
  
  @Override
  public NumberedTable clone() {
    return new NumberedTable(this);
  }
  
  
  public final int id() {
    return id;
  }

}
