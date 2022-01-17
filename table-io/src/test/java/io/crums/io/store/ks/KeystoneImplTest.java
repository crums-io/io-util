/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.ks;


import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.logging.Logger;

import org.junit.Test;

import com.gnahraf.test.IoTestCase;


/**
 * 
 * @author Babak
 */
public class KeystoneImplTest extends IoTestCase {

  private final Logger log;


  public KeystoneImplTest() {
    log = Logger.getLogger(getClass().getSimpleName());
  }


  @Test
  public void testBadArgs() {
    File testFile = getMethodOutputFilepath(new Object() { });
    verifyCreateKeystoneFails(testFile, -1, 0);
  }


  @Test
  public void testBadArgs2() throws IOException {
    File testFile = getMethodOutputFilepath(new Object() { });
    createKeystone(testFile, 0, 0);
    verifyLoadKeystoneFails(testFile, 5);
  }


  @Test
  public void testRoundtrip() throws IOException {
    File file = getMethodOutputFilepath(new Object() { });
    long initValue = 5;
    Keystone keystone = createKeystone(file, 0, initValue);
    assertEquals(initValue, keystone.get());
  }


  @Test
  public void testRoundtrip2() throws IOException {
    File file = getMethodOutputFilepath(new Object() { });
    long initValue = 5;
    Keystone keystone = createKeystone(file, 0, initValue);
    assertEquals(initValue, keystone.get());
  }


  @Test
  public void testRoundtrip3() throws IOException {
    File file = getMethodOutputFilepath(new Object() { });
    long value = 5;
    Keystone keystone = createKeystone(file, 0, value);
    value = -9;
    keystone.set(value);
    keystone = loadKeystone(file, 0);
    assertEquals(value, keystone.get());

  }


  @Test
  public void testRoundtrip4() throws IOException {
    File file = getMethodOutputFilepath(new Object() { });
    long value = 5;
    Keystone keystone = createKeystone(file, 0, value);
    value = -9;
    keystone.set(value);
    value = 11;
    keystone.set(value);
    keystone = loadKeystone(file, 0);
    assertEquals(value, keystone.get());
  }

	@Test
	public void testOldValue() throws IOException {
    File file = getMethodOutputFilepath(new Object() { });
		long oldvalue = 5;
		Keystone keystone = createKeystone(file, 0, oldvalue);
		long newvalue = -9;
		assertEquals(oldvalue, keystone.set(newvalue));
	}


  @Test
  public void testIncrement() throws IOException {
    File file = getMethodOutputFilepath(new Object() { });
    long value = 5;
    Keystone keystone = createKeystone(file, 0, value);
    assertEquals(value, keystone.get());

    long incr = 10;
    value += incr;
    assertEquals(value, keystone.increment(incr));
    assertEquals(value, keystone.get());

    keystone = loadKeystone(file, 0);
    assertEquals(value, keystone.get());
  }


  @Test
  public void testDecrement() throws IOException {
    File file = getMethodOutputFilepath(new Object() { });
    long value = 5;
    Keystone keystone = createKeystone(file, 0, value);
    assertEquals(value, keystone.get());

    long incr = -10;
    value += incr;
    assertEquals(value, keystone.increment(incr));
    assertEquals(value, keystone.get());

    keystone = loadKeystone(file, 0);
    assertEquals(value, keystone.get());
  }


  private Keystone createKeystone(File testFile, long startOffset, long initValue) throws IOException {

    if (testFile.exists())
      fail("test file already exists: " + testFile);
    @SuppressWarnings("resource")
    FileChannel fileChannel = new RandomAccessFile(testFile, "rw").getChannel();
    return createKeystone(fileChannel, startOffset, initValue);
  }
  
  
  protected Keystone createKeystone(
      FileChannel fileChannel, long startOffset, long initValue) throws IOException {
    return new KeystoneImpl(fileChannel, startOffset, initValue);
  }
  
  protected Keystone loadKeystone(
      FileChannel fileChannel, long startOffset) throws IOException {
    return new KeystoneImpl(fileChannel, startOffset);
  }


  private Keystone loadKeystone(File file, long startOffset) throws IOException {
    if (!file.exists())
      fail("test file does not exist: " + file);
    @SuppressWarnings("resource")
    FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
    return loadKeystone(fileChannel, startOffset);
  }


  private void verifyCreateKeystoneFails(File file, long startOffset, long initValue) {
    try {
      createKeystone(file, startOffset, initValue);
      fail();
    } catch (Exception x) {
      // expected
      log.info("expected error: " + x.getMessage() + "; " + x.getClass().getName());
    }
  }


  private void verifyLoadKeystoneFails(File file, long startOffset) throws IOException {
    try {
      loadKeystone(file, startOffset);
      fail();
    } catch (Exception x) {
      // expected
      log.info("expected error: " + x.getMessage() + "; " + x.getClass().getName());
    }
  }

}
