/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util;


import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.crums.testing.SelfAwareTestCase;

/**
 * 
 */
public class CachingListTest extends SelfAwareTestCase {

  
  @Test
  public void testEmpty() {
    List<Integer> empty = new CachingList<Integer>(Collections.emptyList());
    assertTrue(empty.isEmpty());
    assertEquals(0, empty.size());
    try {
      empty.get(0);
      fail();
    } catch (IndexOutOfBoundsException expected) {
      // good
    }
  }
  

  @Test
  public void testIllegalCacheSize() {
    final int factor = 7;
    final int size = 1024*1024;
    List<Integer> source = sequence(factor, size);
    try {
      new CachingList<Integer>(source, 1);
      fail();
    } catch (IllegalArgumentException expected) {
      // good
    }
  }
  
  @Test
  public void testOne() {
    final int factor = 5;
    final int size = 1;
    List<Integer> bigList = sequence(factor, size);
    CachingList<Integer> cachedList = new CachingList<>(bigList);
    
    assertEquals(size, cachedList.size());
    
    Integer zero = cachedList.get(0);
    assertEquals(0, zero.intValue());
    
    assertSame(zero, cachedList.get(0));
  }
  
  @Test
  public void TestBeyondCache() {
    final int factor = 7;
    final int size = 1024*1024;
    List<Integer> source = sequence(factor, size);
    CachingList<Integer> cachedList = new CachingList<>(source);
    final long jumpFactor = 89 * 89 * 89; // relatively prime to *size
    for (int i = 0; i < size; ++i) {
      int index = (int) ((i * jumpFactor) % size);
      assertEquals(source.get(index), cachedList.get(index));
    }
  }
  
  @Test
  public void TestBeyondCacheWith16_4() {
    final int factor = 7;
    final int size = 1024*1024;
    List<Integer> source = sequence(factor, size);
    CachingList<Integer> cachedList = new CachingList<>(source, 16, 4);
    final long jumpFactor = 89 * 89 * 89; // relatively prime to *size
    for (int i = 0; i < size; ++i) {
      int index = (int) ((i * jumpFactor) % size);
      assertEquals(source.get(index), cachedList.get(index));
    }
  }
  
  
  
  private List<Integer> sequence(int factor, int size) {
    return new Lists.RandomAccessList<Integer> () {
      @Override
      public Integer get(int index) {
        return index * factor;
      }
      @Override
      public int size() {
        return size;
      }
    };
  }

}
