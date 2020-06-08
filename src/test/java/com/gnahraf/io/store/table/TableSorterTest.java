/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import com.gnahraf.io.store.table.order.RowOrder;
import com.gnahraf.io.store.table.order.RowOrders;

/**
 * 
 * @author Babak
 */
public class TableSorterTest extends TableTestHarness {
  
  protected final static int ROW_WIDTH = 4;
  protected final static RowOrder ORDER = RowOrders.INT_ORDER;
  
  protected long beginSortNanos;
  protected long endSortNanos;

  @Test
  public void simplest() throws IOException {
    initUnitTestDir(new Object() { });
    int[] values = {
        89, 23
    };
    testImpl(values);
  }

  @Test
  public void testSome() throws IOException {
    initUnitTestDir(new Object() { });
    int[] values = {
        89, 23, 11, 12, 36, 9, 17, 54
    };
    testImpl(values);
  }
  
  // 4 byte rows
  
  
  @Test
  public void test1K() throws IOException {
    initUnitTestDir(new Object() { });
    bigTestImpl(1);
  }
  
  
  @Test
  public void test2K() throws IOException {
    initUnitTestDir(new Object() { });
    bigTestImpl(2);
  }
  
  
  @Test
  public void test4K() throws IOException {
    initUnitTestDir(new Object() { });
    bigTestImpl(4);
  }
  
  
  @Test
  public void test8K() throws IOException {
    initUnitTestDir(new Object() { });
    bigTestImpl(8);
  }
  
  
  @Test
  public void test16K() throws IOException {
    initUnitTestDir(new Object() { });
    bigTestImpl(16);
  }
  
  
  @Test
  public void test32K() throws IOException {
    initUnitTestDir(new Object() { });
    bigTestImpl(32);
  }
  
  
  @Test
  public void test64K() throws IOException {
    initUnitTestDir(new Object() { });
    bigTestImpl(64);
  }

  
  // 8 byte rows
  
  @Test
  public void test1K8() throws IOException {
    initUnitTestDir(new Object() { });
    bigTestImpl(1, 8);
  }
  
  
  @Test
  public void test2K8() throws IOException {
    initUnitTestDir(new Object() { });
    bigTestImpl(2, 8);
  }
  
  
  @Test
  public void test4K8() throws IOException {
    initUnitTestDir(new Object() { });
    bigTestImpl(4, 8);
  }
  
  
  @Test
  public void test8K8() throws IOException {
    initUnitTestDir(new Object() { });
    bigTestImpl(8, 8);
  }
  
  
  @Test
  public void test16K8() throws IOException {
    initUnitTestDir(new Object() { });
    bigTestImpl(16, 8);
  }
  
  
  @Test
  public void test32K8() throws IOException {
    initUnitTestDir(new Object() { });
    bigTestImpl(32, 8);
  }
  
  
  @Test
  public void test64K8() throws IOException {
    initUnitTestDir(new Object() { });
    bigTestImpl(64, 8);
  }
  

  
  // 64 byte rows
  
  @Test
  public void testCount128Width64() throws IOException {
    initUnitTestDir(new Object() { });
    bigTest(128, 64);
  }
  
  
  @Test
  public void testCount256Width64() throws IOException {
    initUnitTestDir(new Object() { });
    bigTest(256, 64);
  }
  
  
  @Test
  public void testCount512Width64() throws IOException {
    initUnitTestDir(new Object() { });
    bigTest(512, 64);
  }
  
  
  @Test
  public void testCount1KWidth64() throws IOException {
    initUnitTestDir(new Object() { });
    bigTest(1024, 64);
  }
  
  
  @Test
  public void testCount2KWidth64() throws IOException {
    initUnitTestDir(new Object() { });
    bigTest(2048, 64);
  }
  
  
  @Test
  public void testCount4KWidth64() throws IOException {
    initUnitTestDir(new Object() { });
    bigTest(4 * 1024, 64);
  }
  
  
  
  

  
  private void bigTestImpl(int kiloBytes) throws IOException {
    bigTestImpl(kiloBytes, ROW_WIDTH);
  }
  
  private void bigTest(int count, int rowWidth) throws IOException {
    bigTestImpl((count * rowWidth) / 1024, rowWidth);
  }
  
  private void bigTestImpl(int kiloBytes, int rowWidth) throws IOException {
    int memory = kiloBytes * 1024;
    int[] values = new int[memory / rowWidth];
    Random random = new Random(memory);
    for (int i = 0; i < values.length; ++i)
      values[i] = random.nextInt();
    testImpl(values, rowWidth);
    long lapMillis = (endSortNanos - beginSortNanos) / 1000000;
    long rowsSortedPerMilli = (long) (((double) values.length) / (endSortNanos - beginSortNanos) * 1000000);
    log.info(
        "[" + getMethod() + "] sorted " + kiloBytes + "kB (" + values.length + " rows) in " +
        lapMillis + " msec. Sort rate: " + rowsSortedPerMilli + " rows per msec");
  }
  
  private void testImpl(int[] values) throws IOException {
    testImpl(values, ROW_WIDTH);
  }
  
  protected void testImpl(int[] values, int rowWidth) throws IOException {
    // initialize the input table with the given rows..
    // (test method returns a SortedTable, but it's lying.. its unsorted)
    Table unsorted = initIntTable(rowWidth, ORDER, values, 0);
    TableSorter sorter = new TableSorter(ByteBuffer.allocate(values.length * rowWidth), ORDER);
    
    File sortedFile = new File(unitTestDir(), "sorted");
    
    beginSortNanos = System.nanoTime();
    sorter.sort(unsorted, sortedFile);
    endSortNanos = System.nanoTime();
    
    unsorted.close();
    @SuppressWarnings("resource")
    Table sorted = Table.newSansKeystoneInstance(
        new RandomAccessFile(sortedFile, "rw").getChannel(), rowWidth);
    // sort the values so that they become the expected sequence
    Arrays.sort(values);
    // verify the expected sequence..
    ByteBuffer rowBuf = ByteBuffer.allocate(rowWidth);
    assertEquals(values.length, sorted.getRowCount());
    for (int i = values.length; i-- > 0; ) {
      rowBuf.clear();
      sorted.read(i, rowBuf);
      rowBuf.flip();
      assertEquals(values[i], rowBuf.getInt());
    }
    sorted.close();
  }

}
