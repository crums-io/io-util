/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.io.buffer;


import static org.junit.jupiter.api.Assertions.*;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.crums.io.Serial;
import io.crums.testing.SelfAwareTestCase;
import io.crums.util.Lists;

/**
 * 
 */
public class PartitioningTest extends SelfAwareTestCase {
  
  
  static void assertEmptyPartition(Partition empty) {
    assertEquals(0, empty.getParts());
    assertTrue(empty.asList().isEmpty());
    try {
      empty.getPart(0);
      fail();
    } catch (IndexOutOfBoundsException expected) { }
    
  }
  
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
    
    assertEmptyPartition(empty);
    assertEquals(0, empty.getBlock().capacity());
    assertTrue(empty.serialSize() > 0);
    assertEquals(empty.serialSize(), empty.serialize().remaining());
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
  
  @Test
  public void testWritePartitionOne() {
    Object label = new Object() {  };
    testWritePartition(label, 0);
    testWritePartition(label, 1);
    testWritePartition(label, 2);
    testWritePartition(label, 3);
  }
  
  @Test
  public void testWritePartition2() {
    Object label = new Object() {  };
    testWritePartition(label, 0, 0);
    testWritePartition(label, 3, 0);
    testWritePartition(label, 0, 3);
    testWritePartition(label, 1, 3);
  }
  
  @Test
  public void testWritePartitionMany() {
    Object label = new Object() {  };
    testWritePartition(label, 0, 0, 511, 6, 11, 1048, 22, 21, 21, 13289, 0, 6);
  }
  
  
  static class MockSerial implements Serial {
    
    static MockSerial load(ByteBuffer in) {
      int size = in.getInt();
      ByteBuffer data = BufferUtils.slice(in, size);
      return new MockSerial(data);
    }
    
    static ByteBuffer loadDataOnly(ByteBuffer in) {
      return load(in).data;
    }
    
    private final ByteBuffer data;
    
    private MockSerial(ByteBuffer data) {
      this.data = data;
    }
    
    MockSerial(int size, Random rand) {
      byte[] bytes = new byte[size];
      rand.nextBytes(bytes);
      this.data = ByteBuffer.wrap(bytes);
    }
    
    ByteBuffer data() {
      return data.asReadOnlyBuffer();
    }

    @Override
    public int serialSize() {
      return 4 + data.capacity();
    }

    @Override
    public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
      out.putInt(data.capacity());
      out.put(data());
      return out;
    }
    
  }
  
  private void testWritePartition(Object methodLabel, Integer... sizes) {
    long seed = method(methodLabel).hashCode();
    Random rand = new Random(seed);
    List<MockSerial> expected = new ArrayList<>(sizes.length);
    int tally = 0;
    for (int size : sizes) {
      expected.add(new MockSerial(size, rand));
      tally += size + 4;
    }
    // allocate *more than enuf; enuf = tally + 4 * (sizes.length + 1)
    ByteBuffer buffer = ByteBuffer.allocate(tally + 4 * (sizes.length + 10));
    Partitioning.writePartition(buffer, expected);
    buffer.clear();
    Partitioning p = Partitioning.load(buffer);
    assertEquals(Lists.map(expected, MockSerial::data), Lists.map(p.asList(), MockSerial::loadDataOnly));
  }
  

}
