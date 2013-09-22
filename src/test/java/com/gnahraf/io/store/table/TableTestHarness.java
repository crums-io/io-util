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

import org.apache.log4j.Logger;

import com.gnahraf.io.store.table.order.RowOrder;
import com.gnahraf.test.TestDirs;
import com.gnahraf.test.TestHelper;

/**
 * Reusable fragment lifted from {@linkplain MergeSortTest}.
 * 
 * @author Babak
 */
public class TableTestHarness {
  
  protected final Logger log = Logger.getLogger(getClass());
  protected final File testDir = TestDirs.getTestDir(getClass());
  
  private File unitTestDir;
  
  protected File unitTestDir() {
    if (unitTestDir == null)
      throw new IllegalStateException("not initialized");
    return unitTestDir;
  }
  
  
  
  
  protected void initUnitTestDir(Object methodAnon) {
    String method = TestHelper.method(methodAnon);
    if (unitTestDir != null)
      fail();
    log.debug("Creating test directory for " + method);
    File dir = new File(testDir, method);
    assertFalse(dir.exists());
    assertTrue( dir.mkdirs() );
    unitTestDir = dir;
  }
  
  
  public String getMethod() {
    return unitTestDir.getName();
  }
  
  protected FileChannel openFile(String filename, boolean exists) throws IOException {
    File testFile = new File(unitTestDir, filename);
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
//      ByteBuffer padding = ByteBuffer.allocate(rowSize - 4);
//      {
//        padding.clear();
//        int value = tableIndex;
//        while (padding.hasRemaining()) {
//          padding.put((byte) value);
//          value = (value + 1) % 256;
//        }
//      }
      
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

}
