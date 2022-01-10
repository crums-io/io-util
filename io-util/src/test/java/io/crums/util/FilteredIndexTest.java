/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util;


import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

/**
 * 
 */
public class FilteredIndexTest {
  
  

  
  @Test
  public void testEmpty() {
    Long[] filtered = {   };
    test(filtered);
  }
  
  @Test
  public void testOne() {
    Long[] filtered = {
        23L,
    };
    test(filtered);
  }
  
  @Test
  public void testOneB() {
    Long[] filtered = {
        0L,
    };
    test(filtered);
  }
  
  @Test
  public void testTwo() {
    Long[] filtered = {
        23L,
        25L,
    };
    test(filtered);
  }
  
  @Test
  public void testTwoConsecutive() {
    Long[] filtered = {
        Integer.MAX_VALUE + 23L,
        Integer.MAX_VALUE + 24L,
    };
    test(filtered);
  }
  
  @Test
  public void testTwoConsecutiveB() {
    Long[] filtered = {
        0L,
        1L,
    };
    test(filtered);
  }
  
  @Test
  public void testThree() {
    Long[] filtered = {
        2L,
        31L,
        Integer.MAX_VALUE + 0L,
    };
    test(filtered);
  }
  
  @Test
  public void testThreeB() {
    Long[] filtered = {
        30L,
        31L,
        Integer.MAX_VALUE + 0L,
    };
    test(filtered);
  }
  
  @Test
  public void testThreeC() {
    Long[] filtered = {
        0L,
        1L,
        Integer.MAX_VALUE + 0L,
    };
    test(filtered);
  }
  
  @Test
  public void testThreeD() {
    Long[] filtered = {
        5L,
        Integer.MAX_VALUE + 0L,
        Integer.MAX_VALUE + 1L,
    };
    test(filtered);
  }
  
  @Test
  public void testThreeE() {
    Long[] filtered = {
        Integer.MAX_VALUE - 1L,
        Integer.MAX_VALUE + 0L,
        Integer.MAX_VALUE + 1L,
    };
    test(filtered);
  }
  
  @Test
  public void testThreeF() {
    Long[] filtered = {
        0L,
        1L,
        2L,
    };
    test(filtered);
  }
  
  private void test(Long[] filtered) {
    FilteredIndex filter = new FilteredIndex(Arrays.asList(filtered));
    for (Long index : filtered)
      testFilterNeighborhood(filter, index);
  }
  
  
  private void testFilterNeighborhood(FilteredIndex filter, long index) {
    long start = index > 0 ? index - 1 : index;
    long end = index + 1;
    for (long i = start; i <= end; ++i)
      testRoundtrip(filter, i);
  }
  
  
  private void testRoundtrip(FilteredIndex filter, long index) {
    long filteredIndex = filter.toFilteredIndex(index);
    assertTrue("index " + index + ", filteredIndex " + filteredIndex, filteredIndex >= -1);
    if (filteredIndex == -1) {
      assertEquals(index + 1, filter.filteredIndicesAhead(index));
      return;
    }
    long unFilteredIndex = filter.toUnfilteredIndex(filteredIndex);
    assertTrue(unFilteredIndex <= index);
    assertTrue(unFilteredIndex >= filteredIndex);
    int filtersAhead = filter.filteredIndicesAhead(unFilteredIndex);
    assertTrue(filtersAhead >= 0);
    assertEquals(filtersAhead, unFilteredIndex - filteredIndex);
  }

}
