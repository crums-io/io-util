/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.util;


import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class NumberListTest {
  
  
  
  @Test
  public void testMinCapEdgeCase() {
    int entriesPerBlock = 1;
    int initCap = 1;
    int valOff = -9;
    int count = 10;
    
    testNsq(entriesPerBlock, initCap, valOff, count);
  }
  
  @Test
  public void testMinCapEdgeCase2() {
    int entriesPerBlock = 2;
    int initCap = 1;
    int valOff = -9;
    int count = 10;
    
    testNsq(entriesPerBlock, initCap, valOff, count);
  }
  
  @Test
  public void testMinCapEdgeCase2_2() {
    int entriesPerBlock = 2;
    int initCap = 2;
    int valOff = -7;
    int count = 10;
    
    testNsq(entriesPerBlock, initCap, valOff, count);
  }
  
  @Test
  public void testMinCapEdgeCase3() {
    int entriesPerBlock = 3;
    int initCap = 1;
    int valOff = 23;
    int count = 10;
    
    testNsq(entriesPerBlock, initCap, valOff, count);
  }
  
  @Test
  public void testMinCapEdgeCase3_2() {
    int entriesPerBlock = 3;
    int initCap = 2;
    int valOff = -23;
    int count = 10;
    
    testNsq(entriesPerBlock, initCap, valOff, count);
  }
  
  @Test
  public void testMinCapEdgeCase3_3() {
    int entriesPerBlock = 3;
    int initCap = 3;
    int valOff = -13;
    int count = 10;
    
    testNsq(entriesPerBlock, initCap, valOff, count);
  }
  
  
  
  @Test
  public void test64k_1M() {
    int entriesPerBlock = 256 * 256;
    int initCap = 128;
    int valOff = 13 * 13 * 13;
    int count = 1024 * 1024;
    
    test(entriesPerBlock, initCap, valOff, count);
  }
  
  
  
  
  
  void test(int entriesPerBlock, int initCap, int valOff, int count) {
    var list = NumberList.newIntList(entriesPerBlock, initCap);
    for (int index = 0; index < count; ++index) {
      list.addInt(index + valOff);
      assertEquals(index + 1, list.size());
    }
    assertValues(list, valOff);
  }
  
  
  void testNsq(int entriesPerBlock, int initCap, int valOff, int count) {
    var list = NumberList.newIntList(entriesPerBlock, initCap);
    for (int index = 0; index < count; ++index) {
      list.addInt(index + valOff);
      assertEquals(index + 1, list.size());
      assertValues(list, valOff);
    }
  }
  
  
  
  void assertValues(NumberList.IntList list, int valOff) {
    for (int index = list.size(); index-- > 0; ) {
      assertEquals(index + valOff, list.getInt(index));
    }
  }
  

}





