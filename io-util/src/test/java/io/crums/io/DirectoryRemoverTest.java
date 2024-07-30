/*
 * Copyright 2009-2020 Babak Farhang
 */
package io.crums.io;


import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;

import io.crums.testing.IoTestCase;

/**
 * 
 */
public class DirectoryRemoverTest extends IoTestCase {

  
  
  File testDir;
  
  
  void initUnitTestDir(Object label) {
    testDir = getMethodOutputFilepath(label);
    assert testDir.mkdirs();
  }
  
  File unitTestDir() {
    return testDir;
  }


  @Test
  public void test01() throws IOException {
    initUnitTestDir(new Object() { });
    File f = new File(unitTestDir(), "root");
    assertTrue( f.createNewFile() );
    DirectoryRemover.removeTree(f);
    assertFalse(f.exists());
  }


  @Test
  public void test02() throws IOException {
    initUnitTestDir(new Object() { });
    File f = new File(unitTestDir(), "root");
    assertTrue( f.mkdir() );
    DirectoryRemover.removeTree(f);
    assertFalse(f.exists());
  }


  @Test
  public void test03() throws IOException {
    initUnitTestDir(new Object() { });
    File f = new File(unitTestDir(), "root");
    assertTrue( f.mkdir() );
    File child = new File(f, "a");
    assertTrue( child.createNewFile());
    DirectoryRemover.removeTree(f);
    assertFalse(f.exists());
  }


  @Test
  public void test04() throws IOException {
    initUnitTestDir(new Object() { });
    File f = new File(unitTestDir(), "root");
    assertTrue( f.mkdir() );
    File child = new File(f, "a");
    assertTrue( child.createNewFile());
    child = new File(f, "b");
    child.mkdir();
    DirectoryRemover.removeTree(f);
    assertFalse(f.exists());
  }


  @Test
  public void test05() throws IOException {
    initUnitTestDir(new Object() { });
    File f = new File(unitTestDir(), "root");
    assertTrue( f.mkdir() );
    File child = new File(f, "a");
    assertTrue( child.createNewFile());
    child = new File(f, "b");
    child.mkdir();
    child = new File(child, "c");
    assertTrue( child.createNewFile());
    DirectoryRemover.removeTree(f);
    assertFalse(f.exists());
  }



}
