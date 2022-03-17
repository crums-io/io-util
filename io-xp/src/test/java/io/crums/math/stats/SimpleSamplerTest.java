/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.math.stats;


import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;


public class SimpleSamplerTest {
  
  private final static double EPSILON = 0.00001;

  @Test
  public void testNone() {
    SimpleSampler sampler = new SimpleSampler();
    assertEquals(0, sampler.getCount());
  }

  @Test
  public void testNone2() {
    SimpleSampler sampler = new SimpleSampler();
    sampler.clear();
    assertEquals(0, sampler.getCount());
  }

  @Test
  public void testOne() {
    SimpleSampler sampler = new SimpleSampler();
    long value = 99;
    sampler.observe(value);
    assertEquals(1, sampler.getCount());
    assertEquals(value, sampler.getMean(), EPSILON);
    assertEquals(0, sampler.getSd(), EPSILON);
    assertEquals(value, sampler.getMin(), EPSILON);
    assertEquals(value, sampler.getMax(), EPSILON);
    
  }

  @Test
  public void testTwo() {
    SimpleSampler sampler = new SimpleSampler();
    long[] values = { 9, 5 };
    for (int i = 0; i < values.length; ++i) {
      sampler.observe(values[i]);
    }
    assertEquals(values.length, sampler.getCount());
    
    assertEquals(7, sampler.getMean(), EPSILON);
  }
  

}
