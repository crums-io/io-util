/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Set;

import com.gnahraf.io.store.table.del.DeleteCodec;
import com.gnahraf.io.store.table.del.MagicNumDeleteCodec;
import com.gnahraf.io.store.table.order.RowOrder;
import com.gnahraf.io.store.table.order.RowOrders;

/**
 * 
 * @author Babak
 */
public class TableTestDHarness extends TableTestHarness {
  
  public final static byte DELETE_MARKER = 1;
  public final static DeleteCodec DELETE_CODEC = MagicNumDeleteCodec.newByteInstance(4, DELETE_MARKER);
  public final static int ROW_WIDTH = 4 + 1 + 4;
  public final static RowOrder ORDER = RowOrders.INT_ORDER;

  
  
  public SortedTable[] createIntTableSet(
      int[][] tableValues, Map<Integer, Integer> expected) throws IOException {
    
    return createIntTableSet(tableValues, expected, null);
  }
  
  
  
  public SortedTable[] createIntTableSet(
      int[][] tableValues, Map<Integer, Integer> expected, Set<Integer> deleted) throws IOException {
    
    SortedTable[] tables = new SortedTable[tableValues.length];
    
    int collisionCount = 0;
    int deleteCount = 0;
    int effectiveDeleteCount = 0;
    int entryCount = 0;

    for (int s = 0; s < tableValues.length; ++s) {
      int[] values = tableValues[s];
      entryCount += values.length;
      
      tables[s] = initIntTable(values, s);
      
      if (expected != null) {
        Integer tableIndex = s;
        for (int v : values) {
          if (v < 0) {
            // deleted
            v = -v - 1;
            ++deleteCount;
            if (expected.remove(v) != null)
              ++effectiveDeleteCount;
            if (deleted != null)
              deleted.add(v);
          } else {
            if (expected.put(v, tableIndex) != null)
              ++collisionCount;
            if (deleted != null)
              deleted.remove(v);
          }
        }
      }
    }
    
    log.info(
        getMethod() + ": collisions/deletes/effective_deletes/all :: " +
            collisionCount + "/" + deleteCount + "/" + effectiveDeleteCount + "/" + entryCount);
    
    return tables;
  }

  protected SortedTable initIntTable(int[] values, int tableIndex) throws IOException {
    return initIntTable(values, tableIndex, null);
  }

  protected SortedTable initIntTable(
      int[] values, int tableIndex, Map<Integer, Integer> expected) throws IOException {
    FileChannel file = openFile(tableIndex, false);
    SortedTable table = new SortedTable(file, ROW_WIDTH, ORDER);
    ByteBuffer rows = ByteBuffer.allocate(ROW_WIDTH * values.length);
    long lastValue = -1;
    for (int i = 0; i < values.length; ++i) {
      boolean delete;
      int value = values[i];
      if (value < 0) {
        delete = true;
        value = -value - 1;
      } else
        delete = false;
      
      assertTrue(lastValue < value);
      lastValue = value;
      
      rows.putInt(value);
      if (delete) {
        rows.put(DELETE_MARKER);
        if (expected != null)
          expected.remove(value);
      } else {
        rows.put((byte) (DELETE_MARKER + 1));
        if (expected != null)
          expected.put(value, tableIndex);
      }
      rows.putInt(tableIndex);
    }
    
    rows.flip();
    assertEquals(rows.capacity(), rows.limit());
    
    // add the rows
    assertEquals(0, table.append(rows) );
    assertEquals(values.length, table.getRowCount());
    
    return table;
  }

  @Override
  protected int guessIntTableIndexFromRow(ByteBuffer row) {
    return row.getInt(5);
  }
  
  
  
}
