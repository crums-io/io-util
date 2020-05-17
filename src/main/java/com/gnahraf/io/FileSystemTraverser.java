/*
 * Copyright 2008-2020 Babak Farhang
 */
package com.gnahraf.io;


import java.io.File;
import java.io.FileFilter;
import java.util.Comparator;
import java.util.function.Consumer;

import com.gnahraf.util.Measure;
import com.gnahraf.util.tree.AbstractTraverser;
import com.gnahraf.util.tree.TraverseListener;

/**
 * A depth-first, directory structure traverser.
 * <p>
 * <em>This class is not safe under concurrent access.</em>
 * </p>
 * 
 * <p><small><i>
 * From the Skwish project at sourceforge.
 * </i></small></p>
 *
 * @see TraverseListener
 * @see Search:
 *      <a href="http://www.google.com/search?q=euler+traversal">Euler
 *      Traversal</a>
 */
public class FileSystemTraverser extends AbstractTraverser<File> {

  private FileFilter filter;






  /**
   * Visits the <tt>root</tt> file, and if a directory, and traverses the
   * directory structure, firing pre-order events along the way. Ordinary
   * files in a same directory are visited <em>before</em> subdirectories.
   * 
   * @param root
   *        the root of the directory structure (must exist)
   * @param visitor
   *        the callback interface invoked on traversal
   * @param filter
   *        optional file filter (may be <tt>null</tt>)
   */
  public static void visitPreorder(
      File root,
      Consumer<File> visitor,
      FileFilter filter)
  {
    visitPreorder(root, visitor, filter, DirectoryOrdering.FILE_FIRST);
  }



  /**
   * Visits the <tt>root</tt> file, and if a directory, and traverses the
   * directory structure, firing pre-order events along the way.
   * 
   * @param root
   *        the root of the directory structure (must exist)
   * @param visitor
   *        the callback interface invoked on traversal
   * @param filter
   *        optional file filter (may be <tt>null</tt>)
   * @param siblingOrder
   *        determines the order files in a same directory get visited. If
   *        <tt>null</tt>, the files' natural ordering is used.
   */
  public static void visitPreorder(
      File root,
      Consumer<File> visitor,
      FileFilter filter,
      Comparator<File> siblingOrder)
  {
    new FileSystemTraverser(root, null, filter, siblingOrder).visitPreorder(visitor);
  }



  /**
   * Visits the <tt>root</tt> file, and if a directory, and traverses the
   * directory structure, firing post-order events along the way. Ordinary
   * files in a same directory are visited <em>after</em> subdirectories.
   * 
   * @param root
   *        the root of the directory structure (must exist)
   * @param visitor
   *        the callback interface invoked on traversal
   * @param filter
   *        optional file filter (may be <tt>null</tt>)
   */
  public static void visitPostorder(
      File root,
      Consumer<File> visitor,
      FileFilter filter)
  {
    visitPostorder(root, visitor, filter, DirectoryOrdering.DIR_FIRST);
  }



  /**
   * Visits the <tt>root</tt> file, and if a directory, and traverses the
   * directory structure, firing post-order events along the way.
   * 
   * @param root
   *        the root of the directory structure (must exist)
   * @param visitor
   *        the callback interface invoked on traversal
   * @param filter
   *        optional file filter (may be <tt>null</tt>)
   * @param siblingOrder
   *        determines the order files in a same directory get visited. If
   *        <tt>null</tt>, the files' natural ordering is used.
   */
  public static void visitPostorder(
      File root,
      Consumer<File> visitor,
      FileFilter filter,
      Comparator<File> siblingOrder)
  {
    new FileSystemTraverser(root, null, filter, siblingOrder).visitPostorder(visitor);
  }






  /**
   * Creates a new instance with the given <tt>root</tt> file (usually a
   * directory).
   */
  public FileSystemTraverser(File root) {
    this(root, null, null, null);
  }


  /**
   * Creates a new instance with the given <tt>root</tt> file (usually a
   * directory).
   */
  public FileSystemTraverser(
      File root,
      TraverseListener<File> listener,
      FileFilter filter,
      Comparator<File> siblingOrder)
  {
    super(root);
    if (!root.exists())
      throw new IllegalArgumentException("root file does not exist: " + root);
    setListener(listener);
    setFilter(filter);
    if (siblingOrder == null)
      siblingOrder = Measure.naturalComparator();
    setSiblingOrder(siblingOrder);
  }

  /**
   * Sets the filter used to determine whether files will be visited.
   */
  public void setFilter(FileFilter filter) {
    this.filter = filter;
  }

  /**
   * Returns the filter used to determine whether files will be visited.
   * May be <tt>null</tt>.
   */
  public FileFilter getFilter() {
    return filter;
  }




  /**
   * Returns the files in the given directory <tt>node</tt>, applying
   * this instance's {@linkplain #setFilter(FileFilter) filter}, if
   * any.
   */
  @Override
  protected File[] getChildren(File node) {
    return node.listFiles(filter);
  }


  /**
   * Returns <tt>true</tt> is the specified file <tt>node</tt> is a
   * directory.
   */
  @Override
  protected boolean hasChildren(File node) {
    return node.isDirectory();
  }


}

