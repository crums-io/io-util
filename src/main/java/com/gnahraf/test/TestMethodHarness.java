/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.test;


import java.io.File;

import org.apache.log4j.Logger;



/**
 * Helper class for creating per-unit-test-method artifact
 * directories.
 * 
 * @author Babak
 */
public class TestMethodHarness {
  
  public final static String PERF_TEST_PROPERTY = "perf_test";

  protected final Logger log = Logger.getLogger(getClass());
  protected final File testDir = TestDirs.getTestDir(getClass());
  private File unitTestDir;

  public TestMethodHarness() {
    super();
  }

  protected final File unitTestDir() {
    if (unitTestDir == null)
      throw new IllegalStateException("not initialized");
    return unitTestDir;
  }

  protected final void initUnitTestDir(Object methodAnon) {
    if (unitTestDir != null)
      throw new IllegalStateException("already set to " + unitTestDir);

    String method = TestHelper.method(methodAnon);
    log.debug("Creating test directory for " + method);
    File dir = new File(testDir, method);
    if (dir.exists())
      throw new IllegalStateException("dir already exists: " + dir);
    if (! dir.mkdirs() )
      throw new RuntimeException("failed to create directory: " + dir.getAbsolutePath());
    unitTestDir = dir;
  }

  public final String getMethod() {
    return unitTestDir().getName();
  }
  
  
  protected boolean skipPerfTest() {
    if (!"true".equalsIgnoreCase(System.getProperty(PERF_TEST_PROPERTY))) {
      log.info("Skipping " + getMethod() + "(): to activate -D" + PERF_TEST_PROPERTY + "=true");
      return true;
    }
    return false;
  }

}
