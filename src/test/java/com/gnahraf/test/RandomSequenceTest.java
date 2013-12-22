package com.gnahraf.test;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.log4j.Logger;
import org.junit.Test;

/*
 * Copyright 2013 Babak Farhang 
 */

/**
 * 
 * @author Babak
 */
public class RandomSequenceTest {
  
  private final static Logger LOG = Logger.getLogger(RandomSequenceTest.class);

  @Test
  public void testPedagogical1() {
    
    long seed = 0;
    int seedRate = 2;
    final long startIndex = 0;
    final int count = 1;
    
    testRange(seed, seedRate, startIndex, count);
  }
  

  @Test
  public void testPedagogical2() {
    
    long seed = 0;
    int seedRate = 2;
    final long startIndex = 1;
    final int count = 1;
    
    testRange(seed, seedRate, startIndex, count);
  }
  

  @Test
  public void testA() {
    
    long seed = 0;
    int seedRate = 13;
    final long startIndex = 11;
    final int count = 1024;
    
    testRange(seed, seedRate, startIndex, count);
  }
  
  
  @Test
  public void testCollisions() {

    long seed = 0;
    int seedRate = 13;
    final long startIndex = 1L + Integer.MAX_VALUE;
    final int count = 1024 * 128;
    
    RandomSequence seq = new RandomSequence(seed, seedRate);
    seq.jumpTo(startIndex);
    assertEquals(startIndex, seq.index());
    
    HashSet<Long> corpus = new HashSet<>();
    for (int countDown = count; countDown-- > 0; )
      corpus.add(seq.next());
    assertEquals(startIndex + count, seq.index());
    
    LOG.info("testCollisions(): " + (count - corpus.size()) + "/" + count + " seedRate=" + seedRate);
  }
  
  /**
   * This takes a couple of seconds.
   */
  @Test
  public void testCollisionsBigger() {

    long seed = 0;
    int seedRate = 13;
    final long startIndex = 1L + Integer.MAX_VALUE;
    final int count = 1024 * 1024 * 4;
    
    RandomSequence seq = new RandomSequence(seed, seedRate);
    seq.jumpTo(startIndex);
    assertEquals(startIndex, seq.index());
    
    HashSet<Long> corpus = new HashSet<>();
    for (int countDown = count; countDown-- > 0; )
      corpus.add(seq.next());
    assertEquals(startIndex + count, seq.index());
    
    LOG.info("testCollisions(): " + (count - corpus.size()) + "/" + count + " seedRate=" + seedRate);
  }
  
  
  
  
  
  
  private void testRange(long seed, int seedRate, long startIndex, int count) {
    RandomSequence seq = new RandomSequence(seed, seedRate);
    seq.jumpTo(startIndex);
    assertEquals(startIndex, seq.index());
    ArrayList<Long> expected = new ArrayList<>();
    for (int countDown = count; countDown-- > 0;)
      expected.add(seq.next());
    assertEquals(startIndex + count, seq.index());
    
    seq.jumpTo(startIndex);
    assertEquals(startIndex, seq.index());
    for (long expectedValue : expected)
      assertEquals(expectedValue, seq.next());
    assertEquals(startIndex + count, seq.index());
    
  }
  
  
  

}
