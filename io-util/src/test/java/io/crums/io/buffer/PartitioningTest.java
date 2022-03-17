/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.io.buffer;


import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.crums.util.Lists;

/**
 * 
 */
public class PartitioningTest {
  
  
  @Test
  public void testEmpty() {
    Partitioning empty = Partitioning.NULL;
    assertEmpty(empty);
    ByteBuffer serialForm = empty.serialize();
    empty = Partitioning.load(serialForm);
    assertEmpty(empty);
    assertTrue(empty.isReadOnly());
  }
  
  
  private void assertEmpty(Partitioning empty) {
    assertEquals(0, empty.getParts());
    assertTrue(empty.asList().isEmpty());
    assertEquals(0, empty.getBlock().capacity());
    try {
      empty.getPart(0);
      fail();
    } catch (IndexOutOfBoundsException expected) { }
    assertTrue(empty.serialSize() > 0);
    ByteBuffer serialForm = empty.serialize();
    assertTrue(serialForm.hasRemaining());
  }
  
  

  @Test
  public void testMinimal() {
    final byte B = (byte) 0xca;
    ByteBuffer block = ByteBuffer.wrap(new byte[] { B } );
    List<Integer> sizes = Collections.singletonList(1);
    Partitioning parts = new Partitioning(block, sizes);
    assertFalse(parts.isReadOnly());
    testMinimal(parts, B);
    ByteBuffer serialForm = parts.serialize();
    parts = Partitioning.load(serialForm);
    testMinimal(parts, B);
  }
  
  
  private void testMinimal(Partitioning parts, byte expected) {
    assertEquals(1, parts.getParts());
    assertEquals(1, parts.getPartSize(0));
    ByteBuffer part = parts.getPart(0);
    assertEquals(0, part.position());
    assertEquals(1, part.limit());
    assertEquals(1, part.capacity());
    assertEquals(expected, part.get());
  }
  
  
  @Test
  public void testMinimalRand() {
    testMany(1, 1);
    testMany(1127, 2);
  }
  
  
  @Test
  public void testAFew() {
    testMany(-1, 13, 2, 503, 46);
    testMany(-1, 13, 0, 503, 46, 0);
  }
  
  
  private void testMany(final long seed, Integer...sizes ) {
    List<Integer> pSizes = Lists.asReadOnlyList(sizes);
    final Integer sum = pSizes.stream().reduce(0, Integer::sum);
    Random random = new Random(seed);
    ByteBuffer block = ByteBuffer.allocate(sum);
    Partitioning parts = new Partitioning(block, pSizes);
    
    final int count = pSizes.size();
    
    assertEquals(count, parts.getParts());
    List<ByteBuffer> rawParts = parts.asList();
    assertEquals(
        sum,
        Lists.map(rawParts, b -> b.remaining()).stream().reduce(0, Integer::sum));
    
    ArrayList<ByteBuffer> expectedParts = new ArrayList<>();
    for (int index = 0; index < count; ++index) {
      random.setSeed(seed + index);
      ByteBuffer part = rawParts.get(index);
      byte[] b = new byte[part.capacity()];
      if (part.hasRemaining()) {
        random.nextBytes(b);
        part.put(b);
      }
      assertFalse(part.hasRemaining());
      ByteBuffer expected = ByteBuffer.wrap(b);
      assertEquals(expected, rawParts.get(index));
      expectedParts.add(expected);
    }
    
    // test serialize and load
    
    parts = Partitioning.load(parts.serialize());
    assertEquals(expectedParts, parts.asList());
    
  }
  

}
