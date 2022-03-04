/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.io.buffer;


import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import io.crums.util.Lists;



/**
 * 
 */
public class NamedPartsTest {
  
  
  private final static String NAME_PREFIX = "p-";
  
  public static String nameForIndex(int index) {
    return NAME_PREFIX + index;
  }
  
  
  @Test
  public void testEmpty() {
    var parts = NamedParts.EMPTY;
    assertSerialTrip(parts);
    assertEquals(0, parts.getParts());
    assertEquals(0, parts.getBlock().capacity());
  }
  
  
  @Test
  public void testMinimal() {
    var parts = newMockInstance(1);
    assertSerialTrip(parts);
    
  }
  
  
  @Test
  public void testOne() {
    test(1003);
  }
  
  
  @Test
  public void testAFew() {
    test(5, 0, 99, 71, 130, 3, 27, 1358);
  }
  
  
  void test(Integer... sizes) {
    var parts = newMockInstance(sizes);
    assertSerialTrip(parts);
    for (int index = 0; index < sizes.length; ++index) {
      assertEquals(sizes[index].intValue(), parts.getPartSize(index));
      assertEquals(parts.getPart(nameForIndex(index)), parts.getPart(index));
    }
  }
  
  public static NamedParts newMockInstance(Integer... sizes) {
    var pSizes = List.of(sizes);
    final int sum = pSizes.stream().reduce(0, Integer::sum);
    var block = new byte[sum];
    new Random(sum).nextBytes(block);
    List<String> names = Lists.map(
        Lists.intRange(0, sizes.length - 1),
        NamedPartsTest::nameForIndex);
    return new NamedParts(ByteBuffer.wrap(block), names, pSizes);
  }
  
  
  public static void assertSerialTrip(NamedParts parts) {
    var buffer = parts.serialize();
    var copy = NamedParts.load(buffer);
    assertEqual(parts, copy);
  }
  
  
  public static void assertEqual(NamedParts expected, NamedParts actual) {
    assertEquals(expected.asList(), actual.asList());
    assertEquals(expected.asMap(), actual.asMap());
  }

}










