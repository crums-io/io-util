/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util;


import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

/**
 * 
 */
public class SetsTest {
  
  
  @Test
  public void testFirstIntersection_00() {
    Integer[] a = { 1, 3, 9, };
    Integer[] b = { 1, 3, 9, };
    int expected = 1;
    
    testFirstIntersection(a, b, expected);
  }
  
  
  @Test
  public void testFirstIntersection_01(){
    Integer[] a = { 1, 3, 9, };
    Integer[] b = { 0, 2, 11, };
//    int expected = 1;
    
    testFirstIntersection(a, b, null);
  }

  
  @Test
  public void testFirstIntersection_02() {
    Integer[] a = { 1, 3, 9, };
    Integer[] b = { 0, 3, 9, };
    int expected = 3;
    
    testFirstIntersection(a, b, expected);
  }

  
  @Test
  public void testFirstIntersection_03() {
    Integer[] a = { 1, 3, 9, };
    Integer[] b = { 0, 9, };
    int expected = 9;
    
    testFirstIntersection(a, b, expected);
  }

  
  @Test
  public void testFirstIntersection_04() {
    Integer[] a = { 1, 3, 9, };
    Integer[] b = { 0, 9, };
    int expected = 9;
    
    testFirstIntersection(a, b, expected);
  }

  
  @Test
  public void testFirstIntersection_05() {
    Integer[] a = { 1, 3, 9, };
    Integer[] b = {  };
//    int expected = 9;
    
    testFirstIntersection(a, b, null);
  }

  
  @Test
  public void testFirstIntersection_06() {
    Integer[] a = {  };
    Integer[] b = {  };
//    int expected = 9;
    
    testFirstIntersection(a, b, null);
  }
  
  
  private void testFirstIntersection(Integer[] a, Integer[] b, Integer expected) {
    testFirstIntersection(Arrays.asList(a), Arrays.asList(b), expected);
  }
  
  private void testFirstIntersection(List<Integer> a, List<Integer> b, Integer expected) {
    SortedSet<Integer> setA = Collections.unmodifiableSortedSet(new TreeSet<>(a));
    SortedSet<Integer> setB = Collections.unmodifiableSortedSet(new TreeSet<>(b));
    assertEquals(expected, Sets.firstIntersection(setA, setB));
    assertEquals(expected, Sets.firstIntersection(setB, setA));
  }
  


  @Test
  public void testSetViewList_Empty() {
    Integer[] ordered = { };
    testSetViewList(ordered);
  }
  
  
  @Test
  public void testSetViewList_Singleton() {
    Integer[] ordered = { 9, };
    testSetViewList(ordered);
  }
  
  
  @Test
  public void testSetViewList_Pair() {
    Integer[] ordered = { -9, 9, };
    testSetViewList(ordered);
  }

  @Test
  public void testSetViewList() {
    Integer[] ordered = { 0, 5, 10, 15, 20, };
    testSetViewList(ordered);
  }
  
  
  
  private void testSetViewList(Integer[] ordered) {
    List<Integer> expected = Arrays.asList(ordered);
    
    SortedSet<Integer> actual = Sets.sortedSetView(expected);
    assertSet(expected, actual);
  }
  
  
  
  private void assertSet(List<Integer> expected, SortedSet<Integer> actual) {
    final int size = expected.size();
    assertEquals(size, actual.size());
    for (int index = 0; index < size; ++index) {
      Integer value = expected.get(index);
      
      SortedSet<Integer> head = actual.headSet(value);
      assertEquals(index, head.size());
      List<Integer> expectedHead = expected.subList(0, index);
      assertElements(expectedHead, head);
      
      // assume gaps
      head = actual.headSet(value - 1);
      assertElements(expectedHead, head);
      
      SortedSet<Integer> tail = actual.tailSet(value);
      assertEquals(size - index, tail.size());
      
      List<Integer> expectedTail = expected.subList(index, size);
      assertElements(expectedTail, tail);
      
      tail = actual.tailSet(value - 1);
      assertElements(expectedTail, tail);
    }
  }
  
  
  private void assertElements(List<Integer> expected, SortedSet<Integer> actual) {
    Iterator<Integer> iExpected = expected.iterator();
    Iterator<Integer> iActual = actual.iterator();
    while (iExpected.hasNext())
      assertEquals(iExpected.next(), iActual.next());
    
    assertFalse(iActual.hasNext());
  }
  
  
  
  @Test
  public void testIntersectionIterator() {
    Integer[] a = { 0,    4, 8, 13,     15, 34, 89, };
    Integer[] b = { 1, 2, 4,    13, 14, 15, 23, 90, 93 };
    Integer[] e = { 4, 13, 15,  };
    
    SortedSet<Integer> sa = new TreeSet<Integer>(Arrays.asList(a));
    SortedSet<Integer> sb = new TreeSet<Integer>(Arrays.asList(b));
    
    Iterator<Integer> expected = Arrays.asList(e).iterator();
    Iterator<Integer> actual = Sets.intersectionIterator(sa, sb);
    while (expected.hasNext())
      assertEquals(expected.next(), actual.next());
    assertFalse(actual.hasNext());
  }
  
  

}











