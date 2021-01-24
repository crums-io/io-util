/*
 * Copyright 2014 Babak Farhang 
 */
package io.crums.test;

import static org.junit.Assert.*;

//import org.junit.Test;

/**
 * Guess not a real test: just demo-ing some spooky auto boxing behavior.
 * 
 * @author Babak
 */
public class AutoBoxingTest {

//  @Test
  public void testAutoIncrement() {
    Integer a = 5;
    ++a;
    assertEquals(6, a.intValue());
    a -= 2;
    assertEquals(4, a.intValue());
    a--;
    assertEquals(3, a.intValue());
  }
  
  
//  @Test
  public void testShiftOps() {
    int i = -1;
    System.out.println(i + " >> 1 : " + Integer.toBinaryString(i >> 1));
    System.out.println(i + " >>> 1 : " + Integer.toBinaryString(i >>> 1));
  }

}
