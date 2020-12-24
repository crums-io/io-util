/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.merge;


import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import io.crums.io.store.table.SortedTable;
import io.crums.io.store.table.TableSet;
import io.crums.io.store.table.TableSetD;
import io.crums.io.store.table.TableTestDHarness;
import io.crums.io.store.table.SortedTable.Searcher;
import io.crums.io.store.table.del.DeleteCodec;
import io.crums.io.store.table.del.MagicNumDeleteCodec;
import io.crums.io.store.table.order.RowOrder;
import io.crums.io.store.table.order.RowOrders;

/**
 * 
 * @author Babak
 */
public class SetMergeSortDTest extends TableTestDHarness {
  
  public final static byte DELETE_MARKER = 1;
  public final static DeleteCodec DELETE_CODEC = MagicNumDeleteCodec.newByteInstance(4, DELETE_MARKER);
  public final static int ROW_WIDTH = 4 + 1 + 4;
  public final static RowOrder ORDER = RowOrders.INT_ORDER;

  @Test
  public void testTheTest() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { 0 , 1 },
        { 2 },
        { 3 },
    };
    testImpl(tableValues, 4);
  }
  

  @Test
  public void testWith1Delete() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { 0 , 1 },
        { -1, 2 },
        { 3 },
    };
    testImpl(tableValues, 3);
  }
  

  @Test
  public void testMixA() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { 0 , 2, 6, 23, 54 },
        { 1, 5, 6, 13, 14, 15, -24, 53, 54 },
        { -1, 2, 4 },
    };
    testImpl(tableValues);
  }
  

  @Test
  public void testMixB() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { 0 , 2, 6, 23, 54 },
        { 5, -8, 22 },
        { 1, 5, 6, 13, 14, 15, -24, 53, 54 },
        { -1, 2, 4 },
    };
    testImpl(tableValues);
  }
  
  private void testImpl(int[][] tableValues) throws IOException {
    testImpl(tableValues, -1);
  }
  
  private void testImpl(int[][] tableValues, int expectedCorpusSize) throws IOException {
    testImpl(tableValues, false, expectedCorpusSize, Integer.MAX_VALUE);
  }
  
  private void testImpl(int[][] tableValues, boolean profile, int expectedCorpusSize, int maxPostTests) throws IOException {
    Map<Integer, Integer> expected = new HashMap<>();
    Set<Integer> deletes = new HashSet<>();
    SortedTable[] stack = createIntTableSet(tableValues, expected, deletes);
    if (expectedCorpusSize >= 0)
      assertEquals(expectedCorpusSize, expected.size());
    SortedTable target;
    {
      FileChannel file = openFile(tableValues.length, false);
      target = new SortedTable(file, stack[0].getRowWidth(), stack[0].order());
    }
    SetMergeSortD sorter = new SetMergeSortD(target, stack, DELETE_CODEC, null);
    
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
      log.info("Collisions & deletions: " + (totalSize - expected.size()) + "/" + totalSize);
      log.info("Total time taken to merge: " + (end - start) / 1000 + " microseconds");
    }

    
    ByteBuffer key = ByteBuffer.allocate(4);
    Searcher searcher = target.newSearcher(8);
    
    start = System.nanoTime();
    for (Map.Entry<Integer, Integer> expectedEntry : expected.entrySet()) {
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
    
    TableSet tableSet = new TableSetD(target, DELETE_CODEC);
    for (int delete : deletes) {
      key.clear();
      key.putInt(delete).flip();
      assertNull(tableSet.getRow(key));
    }
    
    tableSet.close();
    
    end = System.nanoTime();
    if (profile) {
      log.info("Total time taken to test corpus post merge: " + (end - start) / 1000 + " microseconds");
      log.info("===========================");
    }
  }

}
