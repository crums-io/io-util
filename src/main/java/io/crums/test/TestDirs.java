/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;


/**
 * Helper for generating test artifact directories.
 * 
 * @author Babak
 */
public class TestDirs {
  
  private TestDirs() { }

  private final static Logger LOG = Logger.getLogger(TestDirs.class.getName());

  private static File PROC_ROOT_OUTPUT_DIR;
  


  public static synchronized File procTimestampDir() {
    if (PROC_ROOT_OUTPUT_DIR == null) {
      File tsp = newRootTimestampPath();
      String pid = ManagementFactory.getRuntimeMXBean().getName();
      {
        int at = pid.indexOf('@');
        if (at > 0)
          pid = pid.substring(0, at);
      }
      File dir = new File(tsp.getParentFile(), tsp.getName() + "__P" + pid);
      makeArtifactDir(dir);
      PROC_ROOT_OUTPUT_DIR = dir;
      LOG.info("Proc/timestamp dir created: " + dir.getPath());
    }
    return PROC_ROOT_OUTPUT_DIR;
  }


  
  public static File getTestDir(Class<?> clazz) {
    File dir = new File(procTimestampDir(), clazz.getName());
    dir.mkdir();
    if (!dir.isDirectory())
      throw new IllegalStateException("Failed to create test artifact directory: " + dir.getAbsolutePath());
    return dir;
  }


  public static File newRootTimestampDir() {
    File dir = newRootTimestampPath();
    makeArtifactDir(dir);
    return dir;
  }


  public static File newRootTimestampPath() {
    File target = new File("target");
    File artifacts = new File(target, "test-artifacts");
    File timestamp = new File(artifacts, new SimpleDateFormat("yyyy-MM-dd'__T'HH.mm.ss").format(new Date()));
    return timestamp;
  }


  private static void makeArtifactDir(File dir) {
    if (!dir.mkdirs()) {
      throw new IllegalStateException("failed to create artifact dir: " + dir.getAbsolutePath());
    }
  }

}
