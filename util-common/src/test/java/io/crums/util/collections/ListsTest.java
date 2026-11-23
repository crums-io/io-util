/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.util.collections;


import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
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


  // -- isSorted / isSortedNoDups --------------------------------------------

  @Test
  public void testIsSortedNoDups_strictlyAscending() {
    assertTrue(Lists.isSortedNoDups(List.of(1, 2, 3)));
  }

  @Test
  public void testIsSortedNoDups_descendingFails() {
    assertFalse(Lists.isSortedNoDups(List.of(3, 2, 1)));
  }

  @Test
  public void testIsSortedNoDups_duplicateAdjacentFails() {
    // dupsOk == false: a duplicate element must fail, per the method's own
    // "no dups" contract.
    assertFalse(Lists.isSortedNoDups(List.of(1, 2, 2, 3)));
  }

  @Test
  public void testIsSorted_dupsOk_strictlyAscending() {
    assertTrue(Lists.isSorted(List.of(1, 2, 3), true));
  }

  @Test
  public void testIsSorted_dupsOk_duplicateAdjacentPasses() {
    // dupsOk == true: ties are explicitly allowed; only a genuine decrease
    // should fail.
    assertTrue(Lists.isSorted(List.of(1, 2, 2, 3), true));
  }

  @Test
  public void testIsSorted_dupsOk_descendingFails() {
    assertFalse(Lists.isSorted(List.of(1, 3, 2), true));
  }


  // -- binarySearch ----------------------------------------------------------
  //
  // Lists.binarySearch is meant to be a drop-in, behavior-preserving
  // shortcut for Collections.binarySearch (O(1) when the key is outside the
  // list's range, or the list has 3 or fewer elements; delegates otherwise).
  // With unique, sorted elements there's exactly one correct answer, so
  // cross-checking against Collections.binarySearch directly is the
  // strongest test: any divergence is a bug.

  @Test
  public void testBinarySearch_empty() {
    assertEquals(-1, Lists.binarySearch(List.<Integer>of(), 5));
  }

  @Test
  public void testBinarySearch_matchesCollectionsBinarySearch_acrossSizesAndKeys() {
    // sizes 0..9, elements 10,20,...; every key from below the first
    // element to above the last, including every exact element value
    for (int size = 0; size <= 9; ++size) {
      List<Integer> list = new ArrayList<>(size);
      for (int i = 0; i < size; ++i)
        list.add((i + 1) * 10);
      assertBinarySearchMatches(list);
    }
  }

  @Test
  public void testBinarySearch_keyGreaterThanAll_isO1Path() {
    // the fast path this method exists for: Collections.binarySearch's own
    // answer, reached without it ever being called
    List<Integer> list = List.of(10, 20, 30, 40, 50);
    assertEquals(-6, Lists.binarySearch(list, 99));
    assertEquals(Collections.binarySearch(list, 99), Lists.binarySearch(list, 99));
  }

  @Test
  public void testBinarySearch_keyLessThanAll_isFastPath() {
    List<Integer> list = List.of(10, 20, 30, 40, 50);
    assertEquals(-1, Lists.binarySearch(list, 1));
    assertEquals(Collections.binarySearch(list, 1), Lists.binarySearch(list, 1));
  }

  @Test
  public void testBinarySearch_foundAtEachEnd() {
    List<Integer> list = List.of(10, 20, 30, 40, 50);
    assertEquals(0, Lists.binarySearch(list, 10));
    assertEquals(4, Lists.binarySearch(list, 50));
  }

  @Test
  public void testBinarySearch_delegatesForSizeFourOrMore() {
    // size 4+ falls through to Collections.binarySearch for any key not
    // resolved by the endpoint fast-paths; confirm it still agrees with it
    List<Integer> list = List.of(10, 20, 30, 40);
    assertEquals(Collections.binarySearch(list, 25), Lists.binarySearch(list, 25));
    assertEquals(-3, Lists.binarySearch(list, 25));
  }

  @Test
  public void testBinarySearch_duplicates_foundAtEnd() {
    // hiComp/loComp short-circuit on equality before any duplicate ambiguity
    // can arise -- both of these resolve at an endpoint, same as
    // Collections.binarySearch would be free to.
    assertEquals(1, Lists.binarySearch(List.of(5, 5), 5));
    assertEquals(2, Lists.binarySearch(List.of(1, 5, 5), 5));
  }

  /**
   * Asserts {@link Lists#binarySearch(List, Object)} agrees with {@link
   * Collections#binarySearch(List, Object)} for every key spanning one
   * below the first element to one above the last (inclusive of every
   * element's exact value), on a list with unique, ascending elements --
   * where {@code Collections.binarySearch}'s "no guarantee which match"
   * duplicate clause can't apply, so there's exactly one correct answer.
   */
  private void assertBinarySearchMatches(List<Integer> list) {
    int lo = list.isEmpty() ? 0 : list.get(0) - 1;
    int hi = list.isEmpty() ? 0 : list.get(list.size() - 1) + 1;
    for (int key = lo; key <= hi; ++key) {
      int expected = Collections.binarySearch(list, key);
      int actual = Lists.binarySearch(list, key);
      assertEquals(expected, actual, "size=" + list.size() + " key=" + key + " list=" + list);
    }
  }

}




