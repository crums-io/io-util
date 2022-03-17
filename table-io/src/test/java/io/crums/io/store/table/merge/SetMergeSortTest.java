/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.merge;


import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.crums.io.store.table.SortedTable;
import io.crums.io.store.table.TableTestHarness;
import io.crums.io.store.table.SortedTable.Searcher;

/**
 * 
 * @author Babak
 */
public class SetMergeSortTest extends TableTestHarness {

  @Test
  public void testMinimal() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { -1 },
        { 1 },
    };
    testWithTableProvenance(tableValues);
  }

  @Test
  public void test2x2() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { -1 , 1 },
        { 0, 3 },
    };
    testWithTableProvenance(tableValues);
  }

  @Test
  public void test1x2() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { -1 , 1 },
        { 3 },
    };
    testWithTableProvenance(tableValues);
  }

  @Test
  public void test1x2WithCollision() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { -1 , 1 },
        { 1 },
    };
    testWithTableProvenance(tableValues);
  }

  @Test
  public void test1x1x2WithCollision() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { -1 , 1 },
        { 1 },
        { 1 },
    };
    testWithTableProvenance(tableValues);
  }

  @Test
  public void test1x1x2WithCollisionAtStart() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { -1 , 1 },
        { -1 },
        { -1 },
    };
    testWithTableProvenance(tableValues);
  }

  @Test
  public void test1x1x2WithCollisionAtStart2() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { -1 , 2 },
        { 1 },
        { -1 },
    };
    testWithTableProvenance(tableValues);
  }

  @Test
  public void test2x1x2WithCollisionAtEnds() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { -1 , 2 },
        { 1 },
        { -1, 2 },
    };
    testWithTableProvenance(tableValues);
  }

  @Test
  public void test3() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { -1 , 2, 6, 23, 54 },
        { 1, 5, 6, 13, 14, 15, 53, 54 },
        { -1, 2, 4 },
    };
    testWithTableProvenance(tableValues);
  }
  
  @Test
  public void testBigMerge() throws IOException {
    initUnitTestDir(new Object() { });
    int[] tableCounts = {
        1000000,
        10000,
        1000,
    };
    final int numTables = tableCounts.length;
    int[][] tableValues = new int[numTables][];
    for (int i = 0; i < numTables; ++i) {
      IntGenerator gen = new IntGenerator(i, i, (int) Math.pow(7, (i + 1)));
      final int entryCount = tableCounts[i];
      tableValues[i] = new int[entryCount];
      for (int j = 0; j < entryCount; ++j)
        tableValues[i][j] = gen.next();
    }
    testWithTableProvenance(tableValues, true, 100);
  }
  
  @Test
  public void testBigMerge2() throws IOException {
    initUnitTestDir(new Object() { });
    int[] tableCounts = {
        1000000,
        30000,
        10000,
        1000,
    };
    final int numTables = tableCounts.length;
    int[][] tableValues = new int[numTables][];
    for (int i = 0; i < numTables; ++i) {
      IntGenerator gen = new IntGenerator(i, i, (int) Math.min(10000, Math.pow(7, (i + 1))));
      final int entryCount = tableCounts[i];
      tableValues[i] = new int[entryCount];
      for (int j = 0; j < entryCount; ++j)
        tableValues[i][j] = gen.next();
    }
    testWithTableProvenance(tableValues, true, 1000);
  }
  
  
  private void testWithTableProvenance(int[][] tableValues) throws IOException {
    testWithTableProvenance(tableValues, false, Integer.MAX_VALUE);
  }
  
  
  private void testWithTableProvenance(int[][] tableValues, boolean profile, int maxPostTests) throws IOException {
    Map<Integer, Integer> expectedValuesWithTableIds = new HashMap<>();
    final int rowSize = 8;
    SortedTable[] stack = createIntTableSet(rowSize, tableValues, expectedValuesWithTableIds);
    SortedTable target;
    {
      FileChannel file = openFile(tableValues.length, false);
      target = new SortedTable(file, rowSize, stack[0].order());
    }
    SetMergeSort sorter = new SetMergeSort(target, stack);
    
    long start = System.nanoTime();
    
    sorter.mergeToTarget();
    
    long end = System.nanoTime();
    if (profile) {
      log.info("___________________________");
      int tableCount = tableValues.length;
      log.info(getMethod() + ": Merged " + tableCount + " tables");
      ArrayList<Integer> tableSizes = new ArrayList<>(tableCount);
      int totalSize = 0;
      for (int i = 0; i < tableCount; ++i) {
        int size = tableValues[i].length;
        tableSizes.add(size);
        totalSize += size;
      }
      log.info("TableSizes: " + tableSizes);
      log.info("Collisions: " + (totalSize - expectedValuesWithTableIds.size()) + "/" + totalSize);
      log.info("Total time taken to merge: " + (end - start) / 1000 + " microseconds");
    }

    
    ByteBuffer key = ByteBuffer.allocate(4);
    Searcher searcher = target.newSearcher(8);
    
    start = System.nanoTime();
    for (Map.Entry<Integer, Integer> expectedEntry : expectedValuesWithTableIds.entrySet()) {
      if (--maxPostTests == 0)
        break;
      key.clear();
      key.putInt(expectedEntry.getKey()).flip();
      assertTrue( searcher.search(key) );
      ByteBuffer row = searcher.getHitRow();
      assertNotNull(row);
      assertTrue(key.hasRemaining());
      assertEquals(expectedEntry.getValue().intValue(), guessIntTableIndexFromRow(row));
    }
    end = System.nanoTime();
    if (profile) {
      log.info("Total time taken to test corpus post merge: " + (end - start) / 1000 + " microseconds");
      log.info("===========================");
    }
  }
  
  
  

}
