/*
 * Copyright 2008-2020 Babak Farhang
 */
package io.crums.io;


import java.io.File;
import java.io.FileFilter;
import java.util.Comparator;
import java.util.function.Consumer;

import io.crums.util.tree.AbstractTraverser;
import io.crums.util.tree.TraverseListener;

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
 * @see <a href="http://www.google.com/search?q=euler+traversal">Euler
 *      Traversal</a>
 */
public class FileSystemTraverser extends AbstractTraverser<File> {

  private FileFilter filter;






  /**
   * Visits the <code>root</code> file, and if a directory, and traverses the
   * directory structure, firing pre-order events along the way. Ordinary
   * files in a same directory are visited <em>before</em> subdirectories.
   * 
   * @param root
   *        the root of the directory structure (must exist)
   * @param visitor
   *        the callback interface invoked on traversal
   * @param filter
   *        optional file filter (may be <code>null</code>)
   */
  public static void visitPreorder(
      File root,
      Consumer<File> visitor,
      FileFilter filter)
  {
    visitPreorder(root, visitor, filter, DirectoryOrdering.FILE_FIRST);
  }



  /**
   * Visits the <code>root</code> file, and if a directory, and traverses the
   * directory structure, firing pre-order events along the way.
   * 
   * @param root
   *        the root of the directory structure (must exist)
   * @param visitor
   *        the callback interface invoked on traversal
   * @param filter
   *        optional file filter (may be <code>null</code>)
   * @param siblingOrder
   *        determines the order files in a same directory get visited. If
   *        <code>null</code>, the files' natural ordering is used.
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
   * Visits the <code>root</code> file, and if a directory, and traverses the
   * directory structure, firing post-order events along the way. Ordinary
   * files in a same directory are visited <em>after</em> subdirectories.
   * 
   * @param root
   *        the root of the directory structure (must exist)
   * @param visitor
   *        the callback interface invoked on traversal
   * @param filter
   *        optional file filter (may be <code>null</code>)
   */
  public static void visitPostorder(
      File root,
      Consumer<File> visitor,
      FileFilter filter)
  {
    visitPostorder(root, visitor, filter, DirectoryOrdering.DIR_FIRST);
  }



  /**
   * Visits the <code>root</code> file, and if a directory, and traverses the
   * directory structure, firing post-order events along the way.
   * 
   * @param root
   *        the root of the directory structure (must exist)
   * @param visitor
   *        the callback interface invoked on traversal
   * @param filter
   *        optional file filter (may be <code>null</code>)
   * @param siblingOrder
   *        determines the order files in a same directory get visited. If
   *        <code>null</code>, the files' natural ordering is used.
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
   * Creates a new instance with the given <code>root</code> file (usually a
   * directory).
   */
  public FileSystemTraverser(File root) {
    this(root, null, null, null);
  }


  /**
   * Creates a new instance with the given <code>root</code> file (usually a
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
      siblingOrder = Comparator.naturalOrder();
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
   * May be <code>null</code>.
   */
  public FileFilter getFilter() {
    return filter;
  }




  /**
   * Returns the files in the given directory <code>node</code>, applying
   * this instance's {@linkplain #setFilter(FileFilter) filter}, if
   * any.
   */
  @Override
  protected File[] getChildren(File node) {
    return node.listFiles(filter);
  }


  /**
   * Returns <code>true</code> is the specified file <code>node</code> is a
   * directory.
   */
  @Override
  protected boolean hasChildren(File node) {
    return node.isDirectory();
  }


}

