/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import io.crums.io.FileUtils;
import io.crums.io.block.SortedViewBlock;
import io.crums.io.store.table.order.RowOrder;

/**
 * 
 * @author Babak
 */
public class TableSorter {
  
  private final static Logger LOG = System.getLogger(TableSorter.class.getName());
  
  private final ByteBuffer memoryBuffer;
  
  private final RowOrder order;
  

  public TableSorter(ByteBuffer memoryBuffer, RowOrder order) throws IOException {
    this.memoryBuffer = memoryBuffer;
    this.order = order;
    if (memoryBuffer == null)
      throw new IllegalArgumentException("null memoryBuffer");
    if (memoryBuffer.capacity() < 128)
      LOG.log(Level.WARNING, "memory buffer size seems a bit small. Pedantic exercise? " + memoryBuffer);
    if (order == null)
      throw new IllegalArgumentException("null order");
  }
  
  
  public final RowOrder order() {
    return order;
  }
  
  
  public void sort(Table table, File output) throws IOException {
    if (table == null)
      throw new IllegalArgumentException("null table");
    FileUtils.assertDoesntExist(output);
    final long size = table.getRowCount() * table.getRowWidth();
    if (size > memoryBuffer.capacity()) {
      throw new IllegalArgumentException(
          "table byte size > memory buffer size (" + size +
          " > " + memoryBuffer.capacity() + ")");
    }
    if (table.isEmpty())
      throw new IllegalArgumentException(
          "empty table " + table + " with output path " + output);
    memoryBuffer.clear().limit((int) size);
    table.read(0, memoryBuffer);
    memoryBuffer.flip();
    SortedViewBlock blockSorter = new SortedViewBlock(memoryBuffer.slice(), table.getRowWidth(), order);
    
    
    try (
        @SuppressWarnings("resource")
        FileChannel out = new FileOutputStream(output).getChannel()) {
      
      blockSorter.writeSortedCells(out);
    }
  }

}
