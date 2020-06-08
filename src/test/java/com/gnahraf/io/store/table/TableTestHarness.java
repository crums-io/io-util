/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table;


import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Random;


import com.gnahraf.io.store.table.merge.ListMergeSortTest;
import com.gnahraf.io.store.table.order.RowOrder;
import com.gnahraf.io.store.table.order.RowOrders;
import com.gnahraf.test.TestMethodHarness;

/**
 * Reusable fragment lifted from {@linkplain ListMergeSortTest}.
 * 
 * @author Babak
 */
public class TableTestHarness extends TestMethodHarness {
  
  
  public static class IntGenerator {
    
    private final Random rand;
    
    private final int maxSeparation;
    
    private int value;
    
    public IntGenerator(long seed, int start, int maxSeparation) {
      this.rand = new Random(seed);
      this.maxSeparation = maxSeparation;
      this.value = start;
      if (maxSeparation < 2)
        throw new IllegalArgumentException("maxSeparation: " + maxSeparation);
    }
    
    public int next() {
      int out = value;
      value = value + rand.nextInt(maxSeparation) + 1;
      return out;
    }
    
  }
  
  @SuppressWarnings("resource")
  protected FileChannel openFile(String filename, boolean exists) throws IOException {
    File testFile = new File(unitTestDir(), filename);
    if (testFile.exists() != exists)
      fail("test file already exists: " + testFile);
    return new RandomAccessFile(testFile, "rw").getChannel();
  }
  
  
  protected FileChannel openFile(int tableIndex, boolean exists) throws IOException {
    String filename = "table_" + tableIndex;
    return openFile(filename, exists);
  }
  
  
  

  protected SortedTable initIntTable(
      int rowSize, RowOrder order, int[] values, int tableIndex) throws IOException {
    
    assertTrue(rowSize >= 4);
    FileChannel file = openFile(tableIndex, false);
    SortedTable table = new SortedTable(file, rowSize, order);
    ByteBuffer rows = ByteBuffer.allocate(rowSize * values.length);
    
    if (rowSize > 4) {
      
      ByteBuffer padding = paddingForIntTable(rowSize, tableIndex);
      
      for (int i = 0; i < values.length; ++i) {
        rows.putInt(values[i]);
        padding.clear();
        rows.put(padding);
      }
    } else {
      for (int i = 0; i < values.length; ++i)
        rows.putInt(values[i]);
    }
    
    rows.flip();
    assertEquals(rows.capacity(), rows.limit());
    
    // add the rows
    assertEquals(0, table.append(rows) );
    assertEquals(values.length, table.getRowCount());
    
    return table;
  }
  
  
  protected ByteBuffer paddingForIntTable(int rowSize, int tableIndex) {
    ByteBuffer padding = ByteBuffer.allocate(rowSize - 4);
    int value = tableIndex;
    while (padding.hasRemaining()) {
      padding.put((byte) value);
      value = (value + 1) % 256;
    }
    padding.flip();
    return padding;
  }
  
  
  protected int guessIntTableIndexFromRow(ByteBuffer row) {
    assertTrue(row.remaining() > 4);
    return 0xff & row.get(row.position() + 4);
  }




  /**
   * Returns an array of sorted tables generated from the given arguments. An optional
   * map may be passed in order to keep track of the order of the input values.
   * 
   * @param tableValues
   *        per-table values
   * @param expectedValuesWithTableIds
   *        optional union of the table values
   * @return
   *        array of sorted tables just generated
   * @throws IOException
   */
  public SortedTable[] createIntTableSet(int rowSize, int[][] tableValues, Map<Integer, Integer> expectedValuesWithTableIds) throws IOException {
    
    final RowOrder order = RowOrders.INT_ORDER;
  
    SortedTable[] tables = new SortedTable[tableValues.length];
    
    int collisionCount = 0;
    int entryCount = 0;
    for (int s = 0; s < tableValues.length; ++s) {
      int[] values = tableValues[s];
      entryCount += values.length;
      
      tables[s] = initIntTable(rowSize, order, values, s);
      
      Integer tableIndex = s;
      
      if (expectedValuesWithTableIds != null)
        for (int v : values) {
          if (expectedValuesWithTableIds.put(v, tableIndex) != null)
            ++collisionCount;
        }
    }
    
    log.info(getMethod() + ": " + collisionCount + "/" + entryCount + " collisions in test");
    
    return tables;
  }

}
