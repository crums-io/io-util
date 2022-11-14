/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.io.sef;


import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.gnahraf.test.IoTestCase;

/**
 * 
 */
public class SortedLongsTest extends IoTestCase {

  
  @Test
  public void testEmpty() throws Exception {
    final Object label = new Object() {  };
    File dir = makeDir(label);
    File f = new File(dir, "empty-no-header");
    testEmpty(f, 0);
    try { testEmpty(f, 1); } catch (IllegalArgumentException expected) { }
    f = new File(dir, "empty-rnd-17b-header");
    testEmpty(f, 16);
    try { testEmpty(f, 17); } catch (IllegalArgumentException expected) { }
  }
  
  
  private void testEmpty(File f, int headerBytes) throws Exception {
    // first time creates it..
    try (var set = createOrLoad(f, 0)) {
      assertTrue(set.isEmpty());
      assertEquals(0L, set.size());
      assertEquals(0L, set.pendingCommits());
      try {
        set.get(0);
      } catch (IndexOutOfBoundsException expected) { }
    }
    // read it back
    try (var set = createOrLoad(f, 0)) {
      assertTrue(set.isEmpty());
      assertEquals(0L, set.size());
      try {
        set.get(0);
      } catch (IndexOutOfBoundsException expected) { }
    }
  }
  
  
  
  private SortedLongs createOrLoad(File f, int headerBytes) throws IOException {
    @SuppressWarnings("resource")
    var ch = new RandomAccessFile(f, "rw").getChannel();
    return new SortedLongs(ch, headerBytes);
  }
  
  
  
  
  private File makeDir(Object label) {
    var dir = getMethodOutputFilepath(label);
    dir.mkdirs();
    assertTrue(dir.isDirectory());
    return dir;
  }
  
  
  
  @Test
  public void testOne() throws Exception {
    final Object label = new Object() {  };
    File dir = makeDir(label);
    File f = new File(dir, "zero-no-header");
    testOne(f, 0L, 0);
    f = new File(dir, "zero-rnd-1b-header");
    testOne(f, 0L, 1);

    f = new File(dir, "one-no-header");
    testOne(f, 1L, 0);
    f = new File(dir, "one-rnd-1b-header");
    testOne(f, 1L, 1);
    f = new File(dir, "two-no-header");
    testOne(f, 2L, 0);
    f = new File(dir, "three-no-header");
    testOne(f, 3L, 0);
    f = new File(dir, "four-no-header");
    testOne(f, 4L, 0);
    f = new File(dir, "five-no-header");
    testOne(f, 5L, 0);
    f = new File(dir, "six-no-header");
    testOne(f, 6L, 0);
    f = new File(dir, "seven-no-header");
    testOne(f, 7L, 0);
    f = new File(dir, "bignum-no-header");
    testOne(f, Long.MAX_VALUE - Integer.MAX_VALUE + 6L, 99);
  }
  
  
  
  private void testOne(File f, long value, int headerBytes) throws IOException {
    
    try (var set = createOrLoad(f, headerBytes)) {
      set.addNext(value);
      assertEquals(1, set.size());
      assertEquals(value, set.get(0));
      // but don't commit!
    }
    try (var set = createOrLoad(f, headerBytes)) {
      assertTrue(set.isEmpty());
      set.addNext(value);
      assertEquals(1, set.size());
      assertEquals(value, set.get(0));
      // commit
      set.commit();
    }
    try (var set = createOrLoad(f, headerBytes)) {
      assertEquals(1, set.size());
      assertEquals(value, set.get(0));
    }
  }
  
  

  
  
  @Test
  public void testTwo() throws Exception {
    final Object label = new Object() {  };
    File dir = makeDir(label);
    long[] values = new long[2];
    for (int index = 0; index < 16; ++index) {
      values[1] = 1L << index;
      testManyByDir(dir, values);
    }
    for (int index = 0; index < 9; ++index) {
      values[0] = 1L << index;
      values[1] = (1L << (index + 2)) - 1;
      testManyByDir(dir, values);
    }
  }
  
  
  
  
  private void testManyByDir(File dir, long[] ascVals) throws IOException {
    int len = ascVals.length;
    File f = new File(dir, "vals_" + ascVals[0] + "-" + ascVals[len - 1] + "_cnt_" + len);
    testManyByFile(f, ascVals);
  }
  
  
  
  private void testManyByFile(File f, long[] ascVals) throws IOException {
    try (var set = createOrLoad(f, 0)) {
      for (long value : ascVals)
        set.addNext(value);
      assertValues(set, ascVals);
      set.commit();
    }
    try (var set = createOrLoad(f, 0)) {
      assertValues(set, ascVals);
    }
  }
  
  
  private void assertValues(SortedLongs set, long[] ascVals) throws IOException {
    assertEquals(ascVals.length, set.size());
    var work = ByteBuffer.wrap(new byte[16]);
    for (int index = 0; index < ascVals.length; ++index)
      assertEquals(ascVals[index], set.get(index, work));
  }
  
  
  
  @Test
  public void testThree() throws IOException {
    final Object label = new Object() {  };
    File dir = makeDir(label);
    long[] values = new long[3];
    for (int index = 0; index < 9; ++index) {
      values[0] = 1L << index;
      values[1] = (1L << (index + 2)) - 1;
      values[2] = (1L << (index + 4) - 2);
      testManyByDir(dir, values);
    }
  }
  
  
  @Test
  public void test1M() throws IOException {
    final Object label = new Object() {  };
    final int count = 1024 * 1024;
    final int deltaRange = 120;
    File dir = makeDir(label);
    Random rand = new Random(23);
    long[] values = new long[count];
    long last = -1;
    for (int index = 0; index < count; ++index) {
      last += rand.nextInt(deltaRange) + 1;
      values[index] = last;
    }
    testManyByDir(dir, values);
    
    last = 1L << 20;
    for (int index = 0; index < count; ++index) {
      last += rand.nextInt(deltaRange) + 1;
      values[index] = last;
    }
    testManyByDir(dir, values);
  }
  
  
}















