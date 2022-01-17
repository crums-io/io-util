/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table;


import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import io.crums.io.store.table.order.RowOrder;
import io.crums.io.store.table.order.RowOrders;

/**
 * 
 * @author Babak
 */
public class TableSetTest extends TableTestHarness {

  @Test
  public void testSimple() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { -1 },
        { 1 },
    };
    simpleTestImpl(tableValues);
  }

  @Test
  public void testWithValue() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { -1 },
        { 1 },
    };
    testWithTableProvenance(tableValues);
  }

  @Test
  public void testSimple2() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { -1 , 1 },
        { 0, 3 },
    };
    simpleTestImpl(tableValues);
  }

  @Test
  public void testSimple3() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { -1 , 1 },
        { 3 },
    };
    simpleTestImpl(tableValues);
  }

  @Test
  public void testWithValue2() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { -1, 1 },
        { 1 },
    };
    testWithTableProvenance(tableValues);
  }
  

  @Test
  public void testMany() throws IOException {
    initUnitTestDir(new Object() { });
    Random rand = new Random(0);
    final int start = 0;
    final int tableCount = 3;
    int[][] tableValues = new int[tableCount][];
    for (int i = 0; i < tableCount; ++i) {
      tableValues[i] = generateValues(start, rand, 3, (int) Math.pow(10, 1 + tableCount - i));
    }
    testWithTableProvenance(tableValues);
  }
  

  @Test
  public void testMany2() throws IOException {
    initUnitTestDir(new Object() { });
    Random rand = new Random(0);
    final int start = 0;
    final int tableCount = 4;
    int[][] tableValues = new int[tableCount][];
    for (int i = 0; i < tableCount; ++i) {
      tableValues[i] = generateValues(start, rand, 3, (int) Math.pow(5, 1 + tableCount - i));
    }
    testWithTableProvenance(tableValues);
  }
  

  @Test
  public void testMany3() throws IOException {
    initUnitTestDir(new Object() { });
    Random rand = new Random(0);
    final int start = 0;
    final int tableCount =5;
    int[][] tableValues = new int[tableCount][];
    for (int i = 0; i < tableCount; ++i) {
      tableValues[i] = generateValues(start, rand, 3, (int) Math.pow(4, 1 + tableCount - i));
    }
    testWithTableProvenance(tableValues);
  }
  
  
  private int[] generateValues(int next, Random rand, int unit, int count) {
    int[] values = new int[count];
    for (int i = 0; i < count; ++i) {
      values[i] = next;
      next += rand.nextInt(unit) + 1;
    }
    return values;
  }
  
  
  private void simpleTestImpl(int[][] tableValues) throws IOException {
    Set<Integer> expectedValues = new HashSet<>();
    SortedTable[] tables = createIntTableSet(tableValues, expectedValues);
    TableSet tableSet = new TableSet(tables);
    ByteBuffer key = ByteBuffer.allocate(4);
    for (int v : expectedValues) {
      key.clear();
      key.putInt(v).flip();
      ByteBuffer row = tableSet.getRow(key);
      assertNotNull(row);
      assertTrue(key.hasRemaining());
      assertEquals(v, row.getInt());
    }
    tableSet.close();
  }
  
  
  private void testWithTableProvenance(int[][] tableValues) throws IOException {
    Map<Integer, Integer> expectedValuesWithTableIds = new HashMap<>();
    SortedTable[] tables = createIntTableSet(8, tableValues, expectedValuesWithTableIds);
    TableSet tableSet = new TableSet(tables);
    ByteBuffer key = ByteBuffer.allocate(4);
    for (Map.Entry<Integer, Integer> expectedEntry : expectedValuesWithTableIds.entrySet()) {
      key.clear();
      key.putInt(expectedEntry.getKey()).flip();
      ByteBuffer row = tableSet.getRow(key);
      assertNotNull(row);
      assertTrue(key.hasRemaining());
      assertEquals(expectedEntry.getValue().intValue(), guessIntTableIndexFromRow(row));
    }
    tableSet.close();
  }
  

  private SortedTable[] createIntTableSet(
      int[][] tableValues, Set<Integer> expectedValues) throws IOException {
    return createIntTableSet(4, tableValues, expectedValues);
  }
  
  /**
   * Returns an array of sorted tables generated from the given arguments. An optional
   * set may be passed in order to keep track of the order of the input values.
   * 
   * @param tableValues
   *        per-table values
   * @param expectedValues
   *        optional union of the table values
   * @return
   *        array of sorted tables just generated
   * @throws IOException
   */
  private SortedTable[] createIntTableSet(
      int rowSize, int[][] tableValues, Set<Integer> expectedValues) throws IOException {
    
    final RowOrder order = RowOrders.INT_ORDER;

    SortedTable[] tables = new SortedTable[tableValues.length];
    
    for (int s = 0; s < tableValues.length; ++s) {
      int[] values = tableValues[s];
      
      tables[s] = initIntTable(rowSize, order, values, s);
      
      if (expectedValues != null)
        for (int v : values)
          expectedValues.add(v);
    }
    
    return tables;
  }

}
