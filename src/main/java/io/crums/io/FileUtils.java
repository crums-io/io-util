/*
 * Copyright 2013 - 2020 Babak Farhang 
 */
package io.crums.io;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.logging.Logger;

import io.crums.io.channels.ChannelUtils;
import io.crums.util.CloseableIterator;

/**
 * File utilities and convenience methods.
 */
public class FileUtils {
  
  public final static long LOAD_AS_STRING_DEFAULT_MAX_FILE_SIZE = 512 * 1024;

  private FileUtils() {  }
  
  
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
  
  public static File ensureDir(File dir) throws IllegalStateException {
    nonNullPath(dir);
    if (dir.isDirectory())
      return dir;
    if (dir.exists())
      throw new IllegalStateException("cannot overwrite ordinary file as dir: " + dir.getAbsolutePath());
    
    if (!dir.mkdirs() && !dir.isDirectory())
      throw new IllegalStateException("failed to create directory: " + dir.getAbsolutePath());
    
    return dir;
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
  
  
  /**
   * Performs the given file move; failing that (after checking the target already exists),
   * it deletes the file that failed to move.
   * 
   * @param src an existing file
   * @param target the target file (the parent directory must exist).
   * @return <tt>true</tt> iff the move succeeded; <tt>false</tt>, if the <tt>target</tt> file already
   *  exists; failing that an <tt>IllegalStateException</tt> is raised
   */
  public static boolean moveOrDelete(File src, File target) {
    if (!src.isFile())
      throw new IllegalArgumentException("src must be an existing file: " + src);
    
    if (src.renameTo(target))
      return true;
    
    if (src.equals(target))
      throw new IllegalArgumentException("src and target are the same: " + src);
    
    if (!target.exists()) {
      if (!target.getParentFile().isDirectory())
        throw new IllegalArgumentException("target's parent directory does not exist. target: " + target);
      else
        throw new IllegalStateException("mv " + src + " " + target + " both failed and does not exist -- maybe a race");
    }
    if (!src.delete())
      Logger.getGlobal().warning("failed to delete src " + src + " following failed mv to " + target);
    
    return false;
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
  
  
  /**
   * Returns an iterator over the given text file's lines. Note the returned iterator
   * must be eventually closed.
   * 
   * @param textFile a text file
   * @return a {@code Iterator<String>} instance that must be eventually closed
   * 
   * @see CloseableIterator
   */
  public static FileLineIterator newLineIterator(File textFile) throws UncheckedIOException {
    return new FileLineIterator(textFile);
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

  
  /**
   * Loads the file to memory.
   * 
   * @param file    the target file
   * 
   * @return buffer positioned at zero with remaining bytes equal to the file length
   */
  public static ByteBuffer loadFileToMemory(File file) throws IOException {
    return loadFileToMemory(file, null);
  }
  
  /**
   * Loads the file to memory.
   * 
   * @param file    the target file
   * @param out    the optional buffer to write to. If provided, then on return it will be flipped, i.e. the remaining bytes will be equal to the file length 
   * 
   * @return buffer positioned at zero with remaining bytes equal to the file length
   */
  public static ByteBuffer loadFileToMemory(File file, ByteBuffer out) throws IOException {
    final long bytes = file.length();
    if (bytes > LOAD_AS_STRING_DEFAULT_MAX_FILE_SIZE && (out == null || out.capacity() < bytes))
      throw new IllegalArgumentException("file size " +  bytes + " for " + file + " exceeds max capacity");
    if (out == null)
      out = ByteBuffer.allocate((int) bytes);
    else {
      if (out.capacity() < bytes)
        throw new IllegalArgumentException("file size " +  bytes + " for " + file + " exceeds buffer capacity " + out.capacity());
      out.clear().limit((int) bytes);
    }
    try (@SuppressWarnings("resource") FileChannel ch = new FileInputStream(file).getChannel()) {
      ChannelUtils.readRemaining(ch, out);
      out.flip();
    }
    return out;
  }
  
  
  public static void writeNewFile(File file, ByteBuffer contents) throws UncheckedIOException {
    if (file.exists())
      throw new IllegalArgumentException(file + " already exists");
    
    try (@SuppressWarnings("resource") FileChannel channel = new FileOutputStream(file).getChannel()) {
      ChannelUtils.writeRemaining(channel, contents);
    } catch (IOException iox) {
      throw new UncheckedIOException("on attempt to write to file " + file, iox);
    }
  }
  
  
  public static boolean trimFileLength(File file, long trimmedLength)
      throws IllegalArgumentException, UncheckedIOException{
    // check arguments
    long currentLength = file.length();
    if (trimmedLength > currentLength)
      throw new IllegalArgumentException(
          "attempt to trim file from " + currentLength + " --> " + trimmedLength + ": " + file);
    else if (trimmedLength == currentLength)
      return false;
    
    try (RandomAccessFile stream = new RandomAccessFile(file, "rw")) {
      stream.setLength(trimmedLength);
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
    return true;
  }


  
  public static void delete(File file) throws IllegalStateException {
    file.delete();
    if (file.exists())
      throw new IllegalStateException("failed to delete " + file.getAbsolutePath());
  }

  
  /**
   * Recursively copies.
   * 
   * @param source       an existing path. If it's a directory, its files/subdirectories
   *                     are recursively copied/created
   * @param target       the target path
   * @return             the number of <em>files</em> (not directories) copied
   */
  public static int copyRecurse(File source, File target) throws IOException {
    return copyRecurse(source, target, false);
  }
  
  
  /**
   * Recursively copies.
   * 
   * @param source       an existing path. If it's a directory, its files/subdirectories
   *                     are recursively copied/created
   * @param target       the target path
   * @param overwrite    if <tt>true</tt>, and <tt>target</tt> is an existing file, then
   *                     the file will be overwritten; o.w. an {@linkplain IllegalArgumentException}
   *                     is raised. If <tt>target</tt> is a directory (and <tt>source</tt> is too),
   *                     then this argument doesn't matter
   * @return             the number of <em>files</em> (not directories) copied
   */
  public static int copyRecurse(File source, File target, boolean overwrite) throws IOException {
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(target, "target");
    if (source.equals(target))
      return 0;
    if (target.getAbsoluteFile().toPath().startsWith(source.getAbsoluteFile().toPath()))
      throw new IllegalArgumentException("target " + target + " is a subpath of source " + source);
    
    return copyRecurseImpl(source, target, overwrite);
  }
  
  
  private static int copyRecurseImpl(File source, File target, boolean overwrite) throws IOException {
    copyImpl(source, target, overwrite);
    if (source.isFile())
      return 1;
    int count = 0;
    String[] subpaths = source.list();
    for (String subpath : subpaths)
      count += copyRecurseImpl(new File(source, subpath), new File(target, subpath), overwrite);
    return count;
  }
  

  
  
  public static void copy(File source, File target) throws IOException {
    copy(source, target, false);
  }
  
  
  public static void copy(File source, File target, boolean overwrite) throws IOException {
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(target, "target");
    if (source.equals(target))
      return;
    copyImpl(source, target, overwrite);
  }
  
  private static void copyImpl(File source, File target, boolean overwrite) throws IOException {
    if (source.isFile()) {
      if (target.exists()) {
        if (target.isDirectory())
          throw new IllegalArgumentException(
              "target " + target + " is a dir while source " + source + " is not");
        if (!overwrite)
          throw new IllegalArgumentException("attempt to overwrite " + target);

        java.nio.file.Files.copy(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
      } else
        java.nio.file.Files.copy(source.toPath(), target.toPath());
    
    } else if (source.isDirectory())
      ensureDir(target);
    else
      throw new IllegalArgumentException("source does not exist: " + source);
  }

}
