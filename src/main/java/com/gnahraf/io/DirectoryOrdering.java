/*
 * Copyright 2020 Babak Farhang
 */
package com.gnahraf.io;


import java.io.File;
import java.util.Comparator;
import java.util.Objects;


/**
 * Represent 2 types of file orderings by differentiating ordinary files
 * from directories. The implementation is used to prioritize how
 * files or directories are visited first by a <tt>FileSystemTraverser</tt> instance.
 *
 * @see FileSystemTraverser#setSiblingOrder(Comparator)
 * @author Babak Farhang
 */
public class DirectoryOrdering implements Comparator<File> {



  /**
   * Represents the ordering: directories first, or files first.
   *
   * @author Babak Farhang
   */
  public static enum Precedence {

    /**
     * Directories first.
     */
    DIR,

    /**
     * Files first.
     */
    FILE;
  }





  /**
   * Directory precedence.
   */
  public final static DirectoryOrdering DIR_FIRST
  = new DirectoryOrdering(Precedence.DIR);

  /**
   * Ordinary file precedence.
   */
  public final static DirectoryOrdering FILE_FIRST
  = new DirectoryOrdering(Precedence.FILE);



  private final Precedence type;


  public DirectoryOrdering(Precedence type) {
    Objects.requireNonNull(type);
    this.type = type;
  }


  public int compare(File f1, File f2) {
    int comp = compareTypes(f1, f2);
    if (comp == 0)
      comp = compareSameType(f1, f2);
    return comp;
  }


  /**
   * Hook for subclass refinement of ordering. Invoked when the 2 files being
   * {@linkplain #compare(File, File) compared} are either both directories,
   * or both ordinary files.  The default implementation just uses the natural
   * ordering of <tt>File</tt>s.
   * 
   * @return <tt>f1.</tt>{@link File#compareTo(File) compareTo}<tt>(f2)</tt>
   */
  protected int compareSameType(File f1, File f2) {
    return f1.compareTo(f2);
  }


  @Override
  public final boolean equals(Object obj) {
    if (obj == this)
      return true;
    else if (obj == null)
      return false;
    else if (getClass().equals(obj.getClass()))
      return this.type == ((DirectoryOrdering) obj).type;
    return false;
  }


  @Override
  public final int hashCode() {
    return type.hashCode();
  }


  private int compareTypes(File f1, File f2) {
    boolean dir1 = f1.isDirectory();
    boolean dir2 = f2.isDirectory();
    if (dir1 == dir2)
      return 0;
    if (dir1)
      return type == Precedence.DIR ? -1 :  1;
    else
      return type == Precedence.DIR ?  1 : -1;
  }

}
