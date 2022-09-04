/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * <p>Opening modes for first accessing (or possibly creating) a resource, files and
 * directories in particular. There are 4 of these, and these are usually first
 * specified from arguments on the command line.
 * </p><p>
 * If used consistently, this also takes care of corner (error) cases when a file
 * that's supposed to exist has been deleted but the code path creates it on demand
 * anyway because it assumes it doesn't need to check.
 * </p>
 */
public enum Opening {
  
  /**
   * Read-only. Must exist obviously.
   */
  READ_ONLY,
  /**
   * Read/write, if exists.
   */
  READ_WRITE_IF_EXISTS,
  /**
   * Create new. Must <em>not</em> exist.
   */
  CREATE,
  /**
   * Read/write, create on demand. This is the default and never fails.
   * It's also the worst choice.
   */
  CREATE_ON_DEMAND;
  
  
  
  /**
   * Ensures on return there's an existing directory at the specified path.
   * Creates it if it's supposed.
   * 
   * @param dir path to directory
   * 
   * @return <code>dir</code>
   */
  public File ensureDir(File dir) {
    checkDir(dir);
    return FileUtils.ensureDir(dir);
  }
  
  
  /**
   * Opens and returns a (random access) <code>FileChannel</code> from the file
   * at the specified path. Creates it if it's supposed (in which event, the
   * parent directories are also created if needed).
   * 
   * @param file path to the file
   * 
   * @throws IllegalArgumentException if this opening does not permit it
   * @throws IOException on a lower level file system error
   */
  @SuppressWarnings("resource")
  public FileChannel openChannel(File file) throws IllegalArgumentException, IOException {
    checkFileArg(file);
    String mode;
    {
      boolean ro = isReadOnly();
      mode = ro ? "r" : "rw";
      if (!ro && !file.exists()) {
        File parent = file.getParentFile();
        if (parent == null)
          parent = file.getAbsoluteFile().getParentFile();
        FileUtils.ensureDir(parent);
      }
    }
    
    // compiler reports this as a leak when it isn't: closing
    // the returned channel also closes the RandomAccessFile
    return new RandomAccessFile(file, mode).getChannel();
  }
  
  
  /**
   * Determines whether the resource is to be opened in read-only mode.
   */
  public boolean isReadOnly() {
    return this == READ_ONLY;
  }
  
  
  /**
   * Determines whether the resource must already exist.
   */
  public boolean exists() {
    return this == READ_WRITE_IF_EXISTS || this == READ_ONLY;
  }
  
  
  
  /**
   * Checks whether the existence or non-existence of the given resource
   * is permitted.
   * 
   * @param exists <code>true</code> if the resource exists; <code>false</code> if it doesn't
   * @param resource used to form the message on throwing the exception
   */
  public void checkAccept(boolean exists, Object resource) {
    if (!accept(exists)) {
      String msg =
          this == CREATE ?
              "resource already exists: " + resource :
                "expected resource does not exist: " + resource;
      throw new IllegalArgumentException(msg);
    }
  }
  
  
  /**
   * Determines whether the existence or non-existence of the given resource
   * is permitted.
   * 
   * @param exists  <code>true</code> if the resource exists; <code>false</code> if it doesn't
   * 
   * @return <code>true</code> if the resource exists <em>and</em> we're not
   *         meant to create a new one; <code>false</code> if it doesn't
   *              exist <em>and</em> this mode doesn't allow for it to be created
   */
  public boolean accept(boolean exists) {
    switch (this) {
    case READ_ONLY:
    case READ_WRITE_IF_EXISTS:
      return exists;
    case CREATE:
      return !exists;
    case CREATE_ON_DEMAND:
      return true;
    default:
      throw new RuntimeException("unaccounted " + this);
    }
  }
  
  
  /**
   * Checks whether the given file exists, and if not whether this opening mode allows
   * it to be created.
   * 
   * @param file a path which may or may not exist
   * 
   * @throws IllegalArgumentException if not accepted
   */
  public void checkFileArg(File file) throws IllegalArgumentException {
    if (!acceptFile(file)) {
      String msg =
          this == CREATE ?
              "file already exists: " + file :
                "expected file does not exist: " + file;
      throw new IllegalArgumentException(msg);
    }
  }
  
  
  /**
   * Checks whether the given directory exists, and if not whether this opening mode allows
   * it to be created.
   * 
   * @param dir a path which may or may not exist
   * 
   * @throws IllegalArgumentException if not accepted
   */
  public void checkDir(File dir) throws IllegalArgumentException {
    if (!acceptDir(dir)) {
      String msg =
          this == CREATE ?
              "directory already exists: " + dir :
                "expected directory does not exist: " + dir;
      throw new IllegalArgumentException(msg);
    }
  }
  
  
  /**
   * Determines whether the given path meets the criteria of this opening.
   * 
   * @param file a path which may or may not exist
   * 
   * @return <code>true</code> if <code>dir</code> is an existing file <em>or</em> the mode
   *        allows the directory to be created
   * 
   * @throws IllegalArgumentException if <code>file</code> is in fact an existing directory
   */
  public boolean acceptFile(File file) throws IllegalArgumentException {
    switch (this) {
    case READ_ONLY:
    case READ_WRITE_IF_EXISTS:
      boolean exists = file.isFile();
      if (!exists)
        checkNotDirectory(file);
      return exists;
    case CREATE:
      return !file.exists();
    case CREATE_ON_DEMAND:
      
      return true;
    default:
      throw new RuntimeException("unaccounted " + this);
    }
  }
  

  
  /**
   * Determines whether the given path meets the criteria of this opening.
   * 
   * @param dir a path which may or may not exist
   * 
   * @return <code>true</code> if <code>dir</code> is an existing directory <em>or</em> the mode
   *        allows the directory to be created
   *        
   * @throws IllegalArgumentException if <code>dir</code> is in fact an existing file
   */
  public boolean acceptDir(File dir) throws IllegalArgumentException {
    switch (this) {
    case READ_ONLY:
    case READ_WRITE_IF_EXISTS:
      boolean exists = dir.isDirectory();
      if (!exists)
        checkNotFile(dir);
      return exists;
    case CREATE:
      return !dir.exists();
    case CREATE_ON_DEMAND:
      return true;
    default:
      throw new RuntimeException("unaccounted " + this);
    }
  }
  
  
  

  
  private void checkNotDirectory(File file) {
    if (file.isDirectory())
      throw new IllegalArgumentException("expected file is a directory: " + file);
  }
  
  private void checkNotFile(File dir) {
    if (dir.isFile())
      throw new IllegalArgumentException("expected directory is a file: " + dir);
  }

}
