/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.CharBuffer;

/**
 * 
 * @author Babak
 */
public class Files {
  
  public final static long LOAD_AS_STRING_DEFAULT_MAX_FILE_SIZE = 512 * 1024;

  private Files() {  }
  
  
  public static void assertDirArg(File dir) throws IllegalArgumentException {
    nonNullPath(dir);
    if (!dir.isDirectory())
      throw new IllegalArgumentException("not a directory: " + dir.getAbsolutePath());
  }
  
  public static void assertDir(File dir) throws FileNotFoundException {
    nonNullPath(dir);
    if (!dir.isDirectory())
      throw new FileNotFoundException("expected a directory: " + dir.getAbsolutePath());
  }
  
  private static void  nonNullPath(File dir) {
    if (dir == null)
      throw new IllegalArgumentException("null path");
  }
  
  public static void ensureDir(File dir) throws IllegalStateException {
    nonNullPath(dir);
    if (dir.isDirectory())
      return;
    if (dir.exists())
      throw new IllegalStateException("cannot overwrite ordinary file as dir: " + dir.getAbsolutePath());
    
    if (!dir.mkdirs() && !dir.isDirectory())
      throw new IllegalStateException("failed to create directory: " + dir.getAbsolutePath());
  }
  
  
  public static void moveToDir(File file, File dir) throws FileNotFoundException, IllegalStateException {
    assertDir(dir);
    assertFile(file);
    File target = new File(dir, file.getName());
    if (!file.renameTo(target))
      throw new IllegalStateException("failed to move " + file.getPath() + " to " + target.getPath());
  }
  
  
  public static void assertFile(File file) throws IllegalArgumentException, FileNotFoundException {
    if (file == null)
      throw new IllegalArgumentException("null file");
    if (!file.isFile()) {
      String message = file.isDirectory() ? "expected file but found directory: " : "expected file does not exist: ";
      message += file.getAbsolutePath();
      throw new FileNotFoundException(message);
    }
  }
  
  
  public static void assertDoesntExist(File file) throws IoStateException {
    if (file == null)
      throw new IllegalArgumentException("null file path");
    if (file.exists())
      throw new IoStateException("path already exists: " + file.getAbsolutePath());
  }
  
  public static String loadAsString(File file) throws IOException {
    return loadAsString(file, LOAD_AS_STRING_DEFAULT_MAX_FILE_SIZE);
  }
  
  public static String loadAsString(File file, long maxFileSize) throws IOException {
    assertFile(file);

    long size = file.length();
    if (size == 0)
      return "";
    
    // sanity + arg check..
    if (size < 0 || size > maxFileSize) {
      throw new IllegalArgumentException(
          "maxFileSize is " + maxFileSize + "; actual file size for " +
          file.getAbsolutePath() + " is " + size);
    }
    
    StringBuilder buffer = new StringBuilder();
    try (Reader reader = new InputStreamReader(new FileInputStream(file))) {
      
      for (CharBuffer cbuf = CharBuffer.allocate(4096); reader.read(cbuf) != -1; cbuf.clear()) {
        cbuf.flip();
        buffer.append(cbuf);
      }
    }
    return buffer.toString();
  }


  public static void delete(File writeAheadFile) throws IllegalStateException {
    writeAheadFile.delete();
    if (writeAheadFile.exists())
      throw new IllegalStateException("failed to delete " + writeAheadFile.getAbsolutePath());
  }

}
