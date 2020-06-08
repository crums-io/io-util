/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table;


import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Test;

/**
 * 
 * @author Babak
 */
public class TableSetDTest extends TableTestDHarness {

  @Test
  public void testWith2() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { 1 },
        { 2 },
    };
    testImpl(tableValues);
  }

  @Test
  public void testWith2Override1() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { 1 },
        { 1 },
    };
    testImpl(tableValues);
  }

  @Test
  public void testWith2IneffectiveDelete1() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { 1 },
        { -1 },
    };
    testImpl(tableValues);
  }

  @Test
  public void testWith3Delete1() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { 1 },
        { -2 },
        { 0 },
    };
    testImpl(tableValues);
  }

  @Test
  public void testWith4Delete1Override1Resurrect1() throws IOException {
    initUnitTestDir(new Object() { });
    int[][] tableValues = {
        { 1 },
        { -2 },
        { 0 },
        { 0, 1 },
    };
    testImpl(tableValues);
  }
  
  
  
  
  private void testImpl(int[][] tableValues) throws IOException {
    testImpl(tableValues, Integer.MAX_VALUE);
  }
  
  
  private void testImpl(int[][] tableValues, int maxTests) throws IOException {
    Map<Integer, Integer> expected = new HashMap<>();
    HashSet<Integer> deletes = new HashSet<>();
    SortedTable[] tables = createIntTableSet(tableValues, expected);
    TableSetD tableSet = new TableSetD(tables, DELETE_CODEC);
    ByteBuffer key = ByteBuffer.allocate(4); // size of int
    for (Map.Entry<Integer, Integer> expectedEntry : expected.entrySet()) {
      if (--maxTests < 0)
        break;
      key.clear();
      key.putInt(expectedEntry.getKey()).flip();
      ByteBuffer row = tableSet.getRow(key);
      assertNotNull(row);
      assertTrue(key.hasRemaining());
      assertEquals(expectedEntry.getValue().intValue(), guessIntTableIndexFromRow(row));
    }
    for (int deleted : deletes) {
      key.clear();
      key.putInt(deleted).flip();
      assertNull(tableSet.getRow(key));
    }
    
    tableSet.close();
  }

}
