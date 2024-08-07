/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table;


import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import io.crums.testing.IoTestCase;

import io.crums.io.store.table.SortedTable.Searcher;
import io.crums.io.store.table.order.RowOrder;
import io.crums.io.store.table.order.RowOrders;
import io.crums.math.stats.SimpleSampler;
import io.crums.test.PerfProf;

public class SortedTableTest extends IoTestCase {
  
  public final static String PERF_TEST_PROPERTY = "perf_test";

  private final Logger log = System.getLogger(getClass().getName());

  private FileChannel file;
  
  private File filepath;


  @SuppressWarnings("resource")
  private void setup(Object label) throws IOException {
    this.filepath = getMethodOutputFilepath(label);
    file = new RandomAccessFile(filepath, "rw").getChannel();
  }
  
  
  




  @AfterEach
  public void tearDown() throws Exception {
    if (file != null)
      file.close();
  }

  @Test
  public void testEmpty() throws IOException {
    // boiler plate file setup..
    setup(new Object() { });

    final int rowSize = 4;
    RowOrder order = RowOrders.INT_ORDER;
    int rowsInBuffer = Searcher.MIN_BUFFER_ROWS;
    int searchKey = 5;
    SortedTable table = new SortedTable(file, rowSize, order);
    assertEquals(0, table.getRowCount());
    assertEquals(rowSize, table.getRowWidth());
    
    // test the searcher..
    ByteBuffer key = ByteBuffer.allocate(rowSize);
    key.putInt(searchKey).flip();

    Searcher searcher = table.newSearcher(rowsInBuffer);
    assertFalse( searcher.search(key) );
    assertFalse( searcher.isHit() );
    assertEquals(-1, searcher.getHitRowNumber());
    
    table.close();
  }

  @Test
  public void testWithOneRow() throws IOException {
    // boiler plate file setup..
    setup(new Object() { });

    final int rowSize = 4;
    RowOrder order = RowOrders.INT_ORDER;
    int[] values = { 9 };
    
    SortedTable table = initIntTable(rowSize, order, values);
    table = reload(table);
    
    
  }

  @Test
  public void testWithOneRowAndSearch() throws IOException {
    // boiler plate file setup..
    setup(new Object() { });

    final int rowSize = 4;
    RowOrder order = RowOrders.INT_ORDER;
    int[] values = { 9 };
    
    int rowsInSearchBuffer = Searcher.MIN_BUFFER_ROWS;
    int[] searchKeys = { 5, 8, 9, 10, -1 };
    int[] expectedSearchResults = { -1, -1, 0, -2, -1 };
    
    SortedTable table = initIntTable(rowSize, order, values);
    table = reload(table);

    // test the searcher..
    Searcher searcher = table.newSearcher(rowsInSearchBuffer);
    doSearchTest(searcher, values, searchKeys, expectedSearchResults);
    
  }

  @Test
  public void testWithTwoRows() throws IOException {
    // boiler plate file setup..
    setup(new Object() { });

    final int rowSize = 4;
    RowOrder order = RowOrders.INT_ORDER;
    int[] values = { 8, 10 };
    
    int rowsInSearchBuffer = Searcher.MIN_BUFFER_ROWS;
    int[] searchKeys = { 5, 8, 9, 10, -11, 11 };
    int[] expectedSearchResults = { -1, 0, -2, 1, -1, -3 };
    
    SortedTable table = initIntTable(rowSize, order, values);
    table = reload(table);

    // test the searcher..
    Searcher searcher = table.newSearcher(rowsInSearchBuffer);
    doSearchTest(searcher, values, searchKeys, expectedSearchResults);
    
    // take a closer look at the state of the searcher..
    // the last search we did was with 11..
    
    // assert that the entire range was loaded
    // (The size of the table is smaller than the buffer, so the entire range
    // must have been loaded. This is not white-box testing: it's expected
    // behavior.)
    assertEquals(values.length, searcher.getRetrievedRowCount());
    
    assertEquals(0, searcher.getFirstRetrievedRowNumber());
    
    // check the contents of the loaded rows
    for (int i = 0; i < values.length; ++i)
      assertEquals(values[i], searcher.getRow(i).getInt());
    
    try {
      searcher.getRow(values.length);
      fail();
    } catch (IndexOutOfBoundsException iobx) {
      // expected
    }
  }
  


  @Test
  public void testWithFiveRows() throws IOException {
    // boiler plate file setup..
    setup(new Object() { });

    final int rowSize = 4;
    RowOrder order = RowOrders.INT_ORDER;
    int[] values = { -23, -4, 2, 7, 51 };
    
    int rowsInSearchBuffer = Searcher.MIN_BUFFER_ROWS;
    int[] searchKeys = { -24, -23, -22, -5, -4, -3, 50, 51, 52 };
    int[] expectedSearchResults = { -1, -0, -2, -2, 1, -3, -5, 4, -6 };
    
    SortedTable table = initIntTable(rowSize, order, values);
    table = reload(table);

    // test the searcher..
    Searcher searcher = table.newSearcher(rowsInSearchBuffer);
    doSearchTest(searcher, values, searchKeys, expectedSearchResults);
    
  }
  


  @Test
  public void testWithFiveRowsB() throws IOException {
    // boiler plate file setup..
    setup(new Object() { });

    final int rowSize = 4;
    RowOrder order = RowOrders.INT_ORDER;
    int[] values = generateValues(5);
    
    int[][] testKeyResults = generateTestKeysAndExpectedResults(values);
    int[] searchKeys = testKeyResults[0];
    int[] expectedSearchResults = testKeyResults[1];

    int rowsInSearchBuffer = Searcher.MIN_BUFFER_ROWS;
    
    SortedTable table = initIntTable(rowSize, order, values);
    table = reload(table);

    // test the searcher..
    Searcher searcher = table.newSearcher(rowsInSearchBuffer);
    doSearchTest(searcher, values, searchKeys, expectedSearchResults);
    
  }
  


  @Test
  public void testWith1KRows() throws IOException {
    // boiler plate file setup..
    setup(new Object() { });

    final int rowSize = 4;
    RowOrder order = RowOrders.INT_ORDER;
    int[] values = generateValues(1000);
    
    int[][] testKeyResults = generateTestKeysAndExpectedResults(values);
    int[] searchKeys = testKeyResults[0];
    int[] expectedSearchResults = testKeyResults[1];

    int rowsInSearchBuffer = Searcher.MIN_BUFFER_ROWS;
    
    SortedTable table = initIntTable(rowSize, order, values);
    table = reload(table);

    // test the searcher..
    Searcher searcher = table.newSearcher(rowsInSearchBuffer);
    doSearchTest(searcher, values, searchKeys, expectedSearchResults);
    
  }
  


  @Test
  public void testWith100KRows() throws IOException {
    // boiler plate file setup..
    final Object label = new Object() { };
    String method = method(label);
    
    if (!"true".equalsIgnoreCase(System.getProperty(PERF_TEST_PROPERTY))) {
      log.log(Level.INFO, "Skipping " + method + "(): to activate set -D" + PERF_TEST_PROPERTY + "=true");
      return;
    }
    log.log(Level.INFO, "*** Benchmarking " + method + " ***");
    
    setup(label);

    final int rowSize = 4;
    RowOrder order = RowOrders.INT_ORDER;
    int[] values = generateValues(100 * 1000);
    
    int[][] testKeyResults = generateTestKeysAndExpectedResults(values);
    int[] searchKeys = testKeyResults[0];
    int[] expectedSearchResults = testKeyResults[1];

    int rowsInSearchBuffer = Searcher.MIN_BUFFER_ROWS;
    
    SortedTable table = initIntTable(rowSize, order, values);
    table = reload(table);

    // test the searcher..
    Searcher searcher = table.newSearcher(rowsInSearchBuffer);
    doSearchTest(searcher, values, searchKeys, expectedSearchResults);
    
    showSearchStats(searcher);
  }

  @Test
  public void testWith1MRows() throws IOException {
    // boiler plate file setup..
    final Object label = new Object() { };
    String method = method(label);
    
    if (!"true".equalsIgnoreCase(System.getProperty(PERF_TEST_PROPERTY))) {
      log.log(Level.INFO, "Skipping " + method + "(): to activate -D" + PERF_TEST_PROPERTY + "=true");
      return;
    }
    log.log(Level.INFO, "*** Benchmarking " + method + " ***");
    
    
    setup(label);

    final int rowSize = 4;
    RowOrder order = RowOrders.INT_ORDER;
    int[] values = generateValues(1000 * 1000, 0, 16);
    
    int[][] testKeyResults = generateTestKeysAndExpectedResults(values);
    int[] searchKeys = testKeyResults[0];
    int[] expectedSearchResults = testKeyResults[1];

    int rowsInSearchBuffer = Searcher.MIN_BUFFER_ROWS;
    
    SortedTable table = initIntTable(rowSize, order, values);
    table = reload(table);

    // test the searcher..
    Searcher searcher = table.newSearcher(rowsInSearchBuffer);
    doSearchTest(searcher, values, searchKeys, expectedSearchResults);
    
    showSearchStats(searcher);
  }
  
  private void showSearchStats(Searcher searcher) {
    log.log(Level.INFO, "Profiler stats.. (in microseconds)");
    showProf(searcher.getProfiler());
    log.log(Level.INFO, "Block search profiler stats.. (in microseconds)");
    showProf(searcher.getBlockSearchProfiler());
    log.log(Level.INFO, "Number of disk reads per request..");
    showStats(searcher.getReadOpStats());
  }


  private void showProf(PerfProf prof) {
    log.log(Level.INFO, "_______________");
    log.log(Level.INFO, "    max: " + prof.getMaxNanos() / 1000);
    log.log(Level.INFO, "    min: " + prof.getMinNanos() / 1000);
    log.log(Level.INFO, "   mean: " + prof.getMeanNanos() / 1000);
    log.log(Level.INFO, "  sigma: " + prof.getNansosSd() / 1000);
    log.log(Level.INFO, "===============");
    log.log(Level.INFO, "  count: " + NUM_FORMAT.format(prof.getCount()));
    log.log(Level.INFO, "  total: " + NUM_FORMAT.format(prof.getSumNanos() / 1000));
    log.log(Level.INFO, "---------------");
  }
  private final static DecimalFormat NUM_FORMAT = new DecimalFormat("#,###");
  
  private void showStats(SimpleSampler stats) {
    log.log(Level.INFO, "_______________");
    log.log(Level.INFO, "    max: " + stats.getMax());
    log.log(Level.INFO, "    min: " + stats.getMin());
    log.log(Level.INFO, "   mean: " + stats.getMean());
    log.log(Level.INFO, "  sigma: " + stats.getSd());
    log.log(Level.INFO, "===============");
    log.log(Level.INFO, "  count: " + NUM_FORMAT.format(stats.getCount()));
    log.log(Level.INFO, "---------------");
  }
  
  
  private int[] generateValues(int count) {
    return generateValues(count, 0, 8);
  }
  
  private int[] generateValues(int count, int start, int maxSeparation) {
    ++maxSeparation;
    Random delta = new Random(0);
    int[] values = new int[count];
    values[0] = start;
    for (int i = 1; i < count; ++i) {
      values[i] = values[i - 1] + delta.nextInt(maxSeparation) + 1;
    }
    return values;
  }
  
  
  private int[][] generateTestKeysAndExpectedResults(int[] values) {
    int[][] keyResults = new int[2][];
    int[] keys = new int[values.length * 3];
    int[] expectedResults = new int[keys.length];
    keyResults[0] = keys;
    keyResults[1] = expectedResults;
    
    // for each value, generate 3 test search keys that are delta -1, 0, +1 from the value
    for (int i = 0; i < values.length; ++i) {
      
      final int kOff = i * 3;
      for (int k = 0; k < 3; ++k)
        keys[kOff + k] = values[i] + k - 1;
      
      
      if (i == 0 || values[i - 1] != values[i] - 1)
        expectedResults[kOff] = -i - 1;
      else
        expectedResults[kOff] = i - 1;
      
      expectedResults[kOff + 1] = i;
      
      if (i + 1 == values.length || values[i] + 1 != values[i + 1])
        expectedResults[kOff + 2] = -i - 2;
      else
        expectedResults[kOff + 2] = i + 1;
    }
    
    return keyResults;
  }
  
  
  private void doSearchTest(
      Searcher searcher, int[] values, int[] searchKeys, int[] expectedSearchResults)
          throws IOException {

    // ensure the test is set up correctly
    assertEquals(searchKeys.length, expectedSearchResults.length);
    
    ByteBuffer key = ByteBuffer.allocate(searcher.getTable().getRowWidth());
    for (int i = 0; i < searchKeys.length; ++i) {
      key.putInt(searchKeys[i]).flip();
      
      boolean hit = searcher.search(key);
      assertEquals(
          hit, searcher.isHit(),
          "assertion failure at searchKey[" + i + "]: searcher.search() is " +
          hit + " but searcher.isHit() is " + searcher.isHit());
      int expectedResult = expectedSearchResults[i];
      assertEquals(
          hit, expectedResult >= 0,
          "assertion failure at searchKey[" + i + "]: expectedResult is " +
          expectedResult + " but hit is " + hit);
      
      assertEquals(expectedResult, searcher.getHitRowNumber());
      if (hit) {
        assertTrue(searcher.getRetrievedRowCount() > 0);
        assertEquals(values[expectedResult], searcher.getHitRow().getInt());
      }
    }
  }
  
  
  
  
  
  private SortedTable initIntTable(
      int rowSize, RowOrder order, int[] values) throws IOException {
    
    SortedTable table = new SortedTable(file, rowSize, order);
    ByteBuffer rows = ByteBuffer.allocate(table.getRowWidth() * values.length);
    for (int i = 0; i < values.length; ++i)
      rows.putInt(values[i]);
    rows.flip();
    assertEquals(rows.capacity(), rows.limit());
    
    // add the rows
    assertEquals(0, table.append(rows) );
    assertEquals(values.length, table.getRowCount());
    
    return table;
  }
  
  
  @SuppressWarnings("resource")
  private SortedTable reload(SortedTable table) throws IOException {
    long expectedRowCount = table.getRowCount();
    table.close();
    
    file = new RandomAccessFile(filepath, "rw").getChannel();
    SortedTable newTable = new SortedTable(file, table.getRowWidth(), table.order());
    assertEquals(expectedRowCount, newTable.getRowCount());
    return newTable;
  }

}
