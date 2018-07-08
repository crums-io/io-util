package com.gnahraf.io.store.table.merge;


import static org.junit.Assert.*;
import static com.gnahraf.test.TestHelper.method;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Logger;

import org.junit.Test;

import com.gnahraf.io.store.table.SortedTable;
import com.gnahraf.io.store.table.merge.ListMergeSort;
import com.gnahraf.io.store.table.order.RowOrder;
import com.gnahraf.io.store.table.order.RowOrders;
import com.gnahraf.test.TestDirs;

/**
 * 
 * 
 * @author Babak
 */
public class ListMergeSortTest {
  
  private final static Logger LOG = Logger.getLogger(ListMergeSortTest.class.getName());
  private final static File TEST_DIR = TestDirs.getTestDir(ListMergeSortTest.class);
  
  
  private File unitTestDir;
  
  private void initUnitTestDir(String method) {
    if (unitTestDir != null)
      fail();
    LOG.fine("Creating test directory for " + method);
    File dir = new File(TEST_DIR, method);
    assertFalse(dir.exists());
    assertTrue( dir.mkdirs() );
    unitTestDir = dir;
  }
  
  private FileChannel openFile(String filename, boolean exists) throws IOException {
    File testFile = new File(unitTestDir, filename);
    if (testFile.exists() != exists)
      fail("test file already exists: " + testFile);
    return new RandomAccessFile(testFile, "rw").getChannel();
  }
  

  @Test
  public void testSimplestMerge() throws IOException {
    initUnitTestDir(method(new Object() { }));
    
    int[][] sourceValues = {
        { 5, },
        { 9, },
    };
    
    mergeTestUnique(sourceValues);
  }
  

  @Test
  public void testBiMerge2() throws IOException {
    initUnitTestDir(method(new Object() { }));
    
    int[][] sourceValues = {
        { 5, 11, },
        { 9, },
    };
    
    mergeTestUnique(sourceValues);
  }
  

  @Test
  public void testBiMerge3() throws IOException {
    initUnitTestDir(method(new Object() { }));
    
    int[][] sourceValues = {
        { 5, 11, },
        { 9, 13, },
    };
    
    mergeTestUnique(sourceValues);
  }
  

  @Test
  public void testBiMerge4() throws IOException {
    initUnitTestDir(method(new Object() { }));
    
    int[][] sourceValues = {
        { 14, 15},
        { 9, 13, 17 },
    };
    
    mergeTestUnique(sourceValues);
  }
  

  @Test
  public void testBiMerge5() throws IOException {
    initUnitTestDir(method(new Object() { }));
    
    int[][] sourceValues = {
        { 14, 14, 15},
        { 9, 13, 17 },
    };
    
    mergeTest(sourceValues);
  }
  

  @Test
  public void testBiMerge6() throws IOException {
    initUnitTestDir(method(new Object() { }));
    
    int[][] sourceValues = {
        { 14, 14, 15, 17, 45, 80 },
        { 9, 13, 17, 56, 61, 62, 62, 62, },
    };
    
    mergeTest(sourceValues);
  }
  

  @Test
  public void testTriMerge() throws IOException {
    initUnitTestDir(method(new Object() { }));
    
    int[][] sourceValues = {
        { 1, },
        { 3, },
        { 2, },
    };
    
    mergeTest(sourceValues);
  }
  

  @Test
  public void testTriMerge2() throws IOException {
    initUnitTestDir(method(new Object() { }));
    
    int[][] sourceValues = {
        { 1, 5, 5, 7, 84},
        { 3, 4, 5, 11, 23, 29, 31, 37},
        { 2, 4, 22, 23, 24, 65, 90, },
    };
    
    mergeTest(sourceValues);
  }
  

  @Test
  public void testSimpleQuadMerge() throws IOException {
    initUnitTestDir(method(new Object() { }));
    
    int[][] sourceValues = {
        { 5, },
        { 3, },
        { 4, },
        { 1, },
    };
    
    mergeTest(sourceValues);
  }
  

  @Test
  public void testQuadMergeWithDups() throws IOException {
    initUnitTestDir(method(new Object() { }));
    
    int[][] sourceValues = {
        { 5, },
        { 0, 2, },
        { 2, 3, },
        { 1, 2, 2, },
    };
    
    mergeTest(sourceValues);
  }
  

  @Test
  public void testQuadMerge2() throws IOException {
    initUnitTestDir(method(new Object() { }));
    
    int[][] sourceValues = {
        { 1, 5, 5, 7, 84},
        { 3, 4, 5, 11, 23, 29, 31, 37},
        { 2, 4, 22, 23, 24, 62, 90, },
        { 9, 13, 17, 56, 61, 62, 62, 62, },
    };
    
    mergeTest(sourceValues);
  }
  

  @Test
  public void testQuadMerge3() throws IOException {
    initUnitTestDir(method(new Object() { }));
    
    int[][] sourceValues = {
        { 2, 4, 22, 23, 24, 62, 90, },
        { 9, 13, 17, 56, 61, 62, 62, 62, },
        { 1, 5, 5, 7, 84},
        { 3, 4, 5, 11, 23, 29, 31, 37},
    };
    
    mergeTest(sourceValues);
  }
  
  
  
  
  
  
  
  
  /**
   * Doesn't work with dups in the given <tt>sourceValues</tt>;
   * {@linkplain #mergeTest(int[][])} can handle dups. Keeping this around
   * because it makes reading the code for the dup handling clearer.
   */
  private void mergeTestUnique(int[][] sourceValues) throws IOException {
    
    final RowOrder order = RowOrders.INT_ORDER;
    final int rowSize = 4;

    SortedTable[] sources = new SortedTable[sourceValues.length];
    

    SortedSet<Integer> expectedMergedValues = new TreeSet<>();
    
    for (int s = 0; s < sourceValues.length; ++s) {
      
      String filename = "source_" + s;
      FileChannel file = openFile(filename, false);
      int[] values = sourceValues[s];
      
      sources[s] = initIntTable(file, order, values);
      
      for (int v : values)
        expectedMergedValues.add(v);
    }
    
    SortedTable target;
    {
      FileChannel file = openFile("target", false);
      target = new SortedTable(file, rowSize, order);
    }
    
    
    
    ListMergeSort merge = new ListMergeSort(target, sources);
    merge.mergeToTarget();
    final int expectedRowCount = expectedMergedValues.size();
    assertEquals(expectedRowCount, target.getRowCount());
    
    target = reloadTarget(target);
    
    ByteBuffer sampleRow = ByteBuffer.allocate(rowSize);
    
    int rowNumber = 0;
    for (int expectedRowValue : expectedMergedValues) {
      sampleRow.clear();
      target.read(rowNumber, sampleRow);
      sampleRow.flip();
      assertEquals(expectedRowValue, sampleRow.getInt(0));
      ++rowNumber;
    }
    
    
    
    
    // clean up
    target.close();
    for (int i = sources.length; i-- > 0; ) {
      sources[i].close();
    }
  }

  
  /**
   * Test supports dups in values.
   */
  private void mergeTest(int[][] sourceValues) throws IOException {
    
    final RowOrder order = RowOrders.INT_ORDER;
    final int rowSize = 4;

    SortedTable[] sources = new SortedTable[sourceValues.length];
    

    SortedMap<Integer, Integer> expectedValueFreqs = new TreeMap<>();
    
    
    int valuesCount = 0;
    for (int s = 0; s < sourceValues.length; ++s) {
      
      String filename = "source_" + s;
      FileChannel file = openFile(filename, false);
      int[] values = sourceValues[s];
      
      sources[s] = initIntTable(file, order, values);
      
      
      for (int v : values) {
        ++valuesCount;
        Integer ov = v;
        Integer freq = expectedValueFreqs.get(ov);
        if (freq == null)
          expectedValueFreqs.put(ov, 1);
        else
          expectedValueFreqs.put(ov, freq + 1);
      }
    }
    
    SortedTable target;
    {
      FileChannel file = openFile("target", false);
      target = new SortedTable(file, rowSize, order);
    }
    
    
    
    ListMergeSort merge = new ListMergeSort(target, sources);
    merge.mergeToTarget();
    
    final int expectedRowCount = valuesCount;
    assertEquals(expectedRowCount, target.getRowCount());
    
    target = reloadTarget(target);
    
    ByteBuffer sampleRow = ByteBuffer.allocate(rowSize);
    
    int rowNumber = 0;
    for (Entry<Integer, Integer> expectedFreqs : expectedValueFreqs.entrySet()) {
      final int expectedValue = expectedFreqs.getKey();
      int freq = expectedFreqs.getValue();
      for (int counter = freq; counter-- > 0; ) {
        sampleRow.clear();
        target.read(rowNumber, sampleRow);
        sampleRow.flip();
        assertEquals(expectedValue, sampleRow.getInt(0));
        ++rowNumber;
      }
    }
    
    LOG.fine("merge.getCompZeroEdgeCase(): " + merge.getCompZeroEdgeCase());
    
    
    // clean up
    target.close();
    for (int i = sources.length; i-- > 0; ) {
      sources[i].close();
    }
  }
  

  
  private SortedTable reloadTarget(SortedTable target) throws IOException {
    target.close();
    FileChannel file = openFile("target", true);
    return new SortedTable(file, target.getRowWidth(), target.order());
  }

  private SortedTable initIntTable(
      FileChannel file, RowOrder order, int[] values) throws IOException {
    
    final int rowSize = 4;
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

}
