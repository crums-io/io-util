/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.io;


import java.io.File;
import java.io.FilenameFilter;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.crums.util.Lists;

/**
 * <p>A simple but extensible utility for encoding a numbered file naming scheme.
 * (Seemed to be writing this piece of code over and over, so . . ). The default
 * naming scheme is just a prefix, followed by a positive number, followed by an extension.
 * </p><p>
 * Note: a more generic <tt>File</tt> (or filename) to <tt>T</tt> base class (then
 * <tt>T extends Number</tt>) is easily with some work--it just seems premature at this time.
 * </p>
 */
public class NumberedFileScheme {
  
  
  /**
   * Creates and returns an instance with the given prefix and no extension.
   */
  public static NumberedFileScheme withPrefix(String prefix) {
    return new NumberedFileScheme(prefix, "");
  }
  

  /**
   * Creates and returns an instance with the given extension and no prefix.
   */
  public static NumberedFileScheme withExt(String ext) {
    return new NumberedFileScheme("", ext);
  }
  
  
  
  
  
  
  
  private final String prefix;
  private final String ext;
  
  // the following 2 filters are stateless (safe for concurrent use)
  
  private final FilenameFilter longFilter;
  private final FilenameFilter intFilter;
  

  /**
   * Full parameter constructor.
   * 
   * @param prefix non-null (empty ok)
   * @param ext     non-null (empty ok)
   */
  public NumberedFileScheme(String prefix, String ext) {
    this.prefix = Objects.requireNonNull(prefix, "null prefix");
    this.ext = Objects.requireNonNull(ext, "null ext");
    
    
    this.longFilter = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return parseLong(name) >= 0;
      }
    };
    
    this.intFilter = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return parseInt(name) >= 0;
      }
    };
  }
  
  
  /**
   * @return non-null but possibly empty
   */
  public final String prefix() {
    return prefix;
  }
  

  /**
   * @return non-null but possibly empty
   */
  public final String extension() {
    return ext;
  }
  
  
  /**
   * Parses and returns the given filename as a non-negative <tt>long</tt>, or -1, if there
   * is an error.
   */
  public long parseLong(String filename) {
    if (filename.length() < prefix.length() + ext.length())
      return -1;
    try {
      long number = Long.parseLong(integralPart(filename));
      return number < 0 ? -1 : number;
    } catch (NumberFormatException nfx) {
      return -1;
    }
  }
  
  
  private String integralPart(String filename) {
    int start = prefix.length();
    int end = filename.length() - ext.length();
    return filename.substring(start, end);
  }
  

  /**
   * Parses and returns the given filename as a non-negative <tt>int</tt>, or -1, if there
   * is an error.
   */
  public int parseInt(String filename) {
    if (filename.length() < prefix.length() + ext.length())
      return -1;
    try {
      int number = Integer.parseInt(integralPart(filename));
      return number < 0 ? -1 : number;
    } catch (NumberFormatException nfx) {
      return -1;
    }
  }
  
  
  public String toFilename(long number) {
    if (number < 0)
      throw new IllegalArgumentException("negative number " + number);
    return number + ext;
  }
  
  
  public FilenameFilter longFilter() {
    return longFilter;
  }
  
  
  public FilenameFilter intFilter() {
    return intFilter;
  }
  
  
  public List<Long> longEntries(File dir) {
    String[] entries = dir.list(longFilter());
    if (entries == null || entries.length == 0)
      return Collections.emptyList();
    if (entries.length == 1)
      return Collections.singletonList(parseLong(entries[0]));
    Long[] numbers = new Long[entries.length];
    for (int index = entries.length; index-- > 0; )
      numbers[index] = parseLong(entries[index]);
    
    return Lists.asReadOnlyList(numbers);
  }
  
  /**
   * Lists the entries in the directory
   * @param dir
   * @return
   */
  public List<Integer> intEntries(File dir) {
    String[] entries = dir.list(longFilter());
    if (entries == null || entries.length == 0)
      return Collections.emptyList();
    if (entries.length == 1)
      return Collections.singletonList(parseInt(entries[0]));
    Integer[] numbers = new Integer[entries.length];
    for (int index = entries.length; index-- > 0; )
      numbers[index] = parseInt(entries[index]);
    
    return Lists.asReadOnlyList(numbers);
  }

}
