/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.util;


import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class ListsTest {

  @Test
  public void testDowncast() {
    List<Integer> intList = List.of(1,2,3);
    List<Number> numList = Lists.downcast(intList);
    assertEquals(intList, numList);
  }

  @Test
  public void testDowncastToInterface() {
    List<String> sList = List.of("a","b","c");
    List<CharSequence> cList = Lists.downcast(sList);
    assertEquals(sList, cList);
  }


  @Test
  public void testUpcast() {
    List<Number> numList = List.of(1,2,3);
    List<Integer> intList = Lists.upcast(numList);
    assertEquals(intList, numList);
  }

  @Test
  public void testConcat() {
    List<Integer> a = List.of(4, 5, 6);
    List<Integer> b = List.of(7);
    List<Integer> expected = new ArrayList<>(a);
    expected.addAll(b);
    var concatView = Lists.concat(a, b);
    assertEquals(expected, concatView);
    
    // do it in reverse..
    expected = new ArrayList<>(b);
    expected.addAll(a);
    concatView = Lists.concat(b, a);
    assertEquals(expected, concatView);
  }
  
  
  @Test
  public void testMultiCat() {
    List<Integer> a = List.of(4, 5, 6, 7);
    List<Integer> b = List.of(8);
    List<Integer> c = List.of(9, 10, 11);
    List<Integer> d = List.of(12, 13);
    
    List<Integer> expected = new ArrayList<>();
    expected.addAll(a);
    expected.addAll(b);
    var concatView = Lists.concatLists(a, b);
    assertEquals(expected, concatView);
    
    expected.addAll(c);
    concatView = Lists.concatLists(a, b, c);
    
    assertEquals(expected, concatView);
    
    expected.addAll(d);
    concatView = new Lists.MultiCatList<>(List.of(a, b, c, d), true);
    assertEquals(expected, concatView);
  }
  
  
  
  @Test
  public void testRemoveIndex() {
    testRemoveIndex(List.of(99));
    testRemoveIndex(List.of(99, 100));
    
    List<Integer> in = List.of(10, 11, 12, 13, 14);
    testRemoveIndex(in);
    
    var sansIndex2 = Lists.removeIndex(2, in);
    try {
      sansIndex2.get(4);
    } catch (IndexOutOfBoundsException expected) {
      System.out.println("Expected error (but note off-by-one): " + expected);
    }
    
    try {
      Lists.removeIndex(in.size(), in);
    } catch (IndexOutOfBoundsException expected) {
      System.out.println("Expected error: " + expected);
    }
  }
  
  
  private <T> void testRemoveIndex(List<T> in) {
    for (int i = 0; i < in.size(); ++i)
      testRemoveIndex(in, i);
  }
  
  private <T> void testRemoveIndex(List<T> in, int i) {
    List<T> rm = Lists.removeIndex(i, in);
    assertRemoved(in, rm, i);
  }
  
  private <T> void assertRemoved(List<T> in, List<T> rm,  int i) {
    ArrayList<T> expected = new ArrayList<>(in);
    expected.remove(i);
    assertEquals(expected, rm);
  }
  
  

}




