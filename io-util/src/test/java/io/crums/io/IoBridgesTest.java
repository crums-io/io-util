/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.io;


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
public class IoBridgesTest extends IoTestCase {

  @Test
  public void testSliceInputStream() throws Exception {
    final Object label = new Object() { };
    var file = new File(makeTestDir(label), "slice-test.random");
    ByteBuffer fileBytes = writeRandomFile(file, 257, -22);
    
    int[] offsets = { 27, 99, 100, 100, 127 };
    try (var raf = new RandomAccessFile(file, "r")) {
      for (int off = 0, index = 0; index < offsets.length; off = offsets[index++]) {
        int nextOff = offsets[index];
        int len = nextOff - off;
        byte[] slice = IoBridges.sliceInputStream(raf, off, len).readAllBytes();
        fileBytes.clear().position(off).limit(nextOff);
        assertEquals(fileBytes, ByteBuffer.wrap(slice));
      }
    }
  }
  
  
  
  private File makeTestDir(Object label) {
    var dir = getMethodOutputFilepath(label);
    dir.mkdirs();
    assertTrue(dir.isDirectory());
    return dir;
  }
  
  
  private ByteBuffer writeRandomFile(File file, int length, int seed) throws IOException {
    byte[] bytes = randomBytes(length, seed);
    FileUtils.writeNewFile(file, ByteBuffer.wrap(bytes));
    return ByteBuffer.wrap(bytes);
  }
  
  
  private byte[] randomBytes(int length, int seed) {
    byte[] bytes = new byte[length];
    new Random(seed).nextBytes(bytes);
    return bytes;
  }

}
