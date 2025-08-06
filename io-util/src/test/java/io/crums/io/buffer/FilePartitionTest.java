/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.io.buffer;


import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.crums.io.Opening;
import io.crums.io.channels.ChannelUtils;
import io.crums.testing.IoTestCase;

/**
 * 
 */
public class FilePartitionTest extends IoTestCase {
  
  
  @Test
  public void testEmpty() throws Exception {
    var label = new Object() { };
    
    File dir = getMethodOutputFilepath(label);
    File file = new File(dir, "empty-partition");
    
    final var expected = Partitioning.NULL;
    FilePartition fp = writeThenLoad(file, 0, expected, 0);
    assertExpected(expected, fp, file, 0, 0);
  }
  
  
  @Test
  public void testEmptyInMiddle() throws Exception {
    var label = new Object() { };
    
    final int offset = 1057;
    final int postBytes = 99;
    
    File dir = getMethodOutputFilepath(label);
    File file = new File(dir, "empty-in-middle");
    
    final var expected = Partitioning.NULL;
    FilePartition fp = writeThenLoad(file, offset, expected, postBytes);
    
    assertExpected(expected, fp, file, offset, postBytes);
  }
  
  
  @Test
  public void testEmptyAtEnd() throws Exception {
    var label = new Object() { };
    
    final int offset = 1057;
    final int postBytes = 0;
    
    File dir = getMethodOutputFilepath(label);
    File file = new File(dir, "empty-at-end");
    
    final var expected = Partitioning.NULL;
    FilePartition fp = writeThenLoad(file, offset, expected, postBytes);
    
    assertExpected(expected, fp, file, offset, postBytes);
  }
  
  
  
  
  @Test
  public void testEmptySingleton() throws Exception {
    var label = new Object() { };
    
    final int offset = 33;
    final int postBytes = 100;
    
    
    File dir = getMethodOutputFilepath(label);
    File file = new File(dir, "empty-singleton-in-middle");
    
    final var expected = newRandomPartition(0);
    
    FilePartition fp = writeThenLoad(file, offset, expected, postBytes);
    
    assertExpected(expected, fp, file, offset, postBytes);
    
  }
  
  
  
  
  @Test
  public void testOneByte() throws Exception {
    var label = new Object() { };
    
    final int offset = 33;
    final int postBytes = 100;
    
    
    File dir = getMethodOutputFilepath(label);
    File file = new File(dir, "one-byte-one-part");
    
    final var expected = newRandomPartition(1);
    FilePartition fp = writeThenLoad(file, offset, expected, postBytes);
    
    assertExpected(expected, fp, file, offset, postBytes);
    
  }
  
  
  @Test
  public void testOneByte9EmptyParts() throws Exception {
    var label = new Object() { };
    
    final int offset = 0;
    final int postBytes = 0;
    
    doTest(
        label, offset, postBytes,
        0, 0, 0, 1, 0,
        0, 0, 0, 0, 0);
    
  }
  
  @Test
  public void testOneByte9EmptyPartsInBiggerFile() throws Exception {
    var label = new Object() { };
    
    final int offset = 67;
    final int postBytes = 100;
    
    doTest(
        label, offset, postBytes,
        0, 0, 0, 0, 0,
        0, 0, 0, 0, 1);
    
  }
  
  
  @Test
  public void testMany() throws Exception {
    var label = new Object() { };
    
    final int offset = 67;
    final int postBytes = 100;
    
    doTest(
        label, offset, postBytes,
        33, 0, 1024*1024, 70, 10555, 89, 0, 0, 85, 55366,
        0, 0, 0, 0, 1);
    
  }
  
  
  private void doTest(
      Object label, int offset, int postBytes, Integer... sizes)
          throws IOException {
    
    File dir = getMethodOutputFilepath(label);
    File file = new File(
        dir,
        method(label).substring(4) + "-offset_" + offset);
    
    final var expected = newRandomPartition(sizes);
    FilePartition fp = writeThenLoad(file, offset, expected, postBytes);
    
    assertExpected(expected, fp, file, offset, postBytes);
    
  }
  
  
  
  static Partitioning newRandomPartition(Integer... sizes) {
    int sum = Arrays.asList(sizes).stream().reduce(0, Integer::sum);
    Random rand = new Random(sum);
    byte[] block = new byte[sum];
    rand.nextBytes(block);
    return new Partitioning(ByteBuffer.wrap(block), Arrays.asList(sizes));
  }
  
  
  static void assertExpected(
      Partitioning expected, FilePartition actual, File file, int offset, int postBytes) {
    
    assertEqualPartitions(expected, actual);
    assertExpectedFileSize(expected, file, offset, postBytes);
  }
  
  
  static void assertEqualPartitions(Partition expected, Partition actual) {
    assertEquals(expected.asList(), actual.asList());
  }
  
  
  static void assertExpectedFileSize(
      Partitioning expected, File file, int offset, int postBytes) {
    assertEquals(expected.serialSize() + offset + postBytes, (int) file.length());
  }
  
  
  
  private FilePartition writeThenLoad(
      File file, int offset, Partitioning inMem, int postPadSize)
          throws IOException {
    
    file.getParentFile().mkdirs();
    
    Random rand = new Random(file.getName().hashCode());
    
    try (var ch = Opening.CREATE.openChannel(file)) {
      
      if (offset > 0)
        makeNoise(ch, offset, rand);
      
      inMem.writeTo(ch);
      
      if (postPadSize > 0)
        makeNoise(ch, postPadSize, rand);
    }
    
    var ch = Opening.READ_ONLY.openChannel(file);
    return new FilePartition(ch, offset);
  }
  
  
  private void makeNoise(FileChannel ch, int size, Random rand) throws IOException {
    byte[] noise = new byte[size];
    rand.nextBytes(noise);
    ChannelUtils.writeRemaining(ch, ByteBuffer.wrap(noise));
  }

}
