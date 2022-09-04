/*
 * Copyright 2009-2020 Babak Farhang
 */
package io.crums.io;


import java.io.File;
import java.io.IOException;

import io.crums.util.tree.TraverseListener;



/**
 * Recursively removes the contents of a directory tree.
 * Similar to <code>rm myDir -rf</code>.
 * <h2>Warning!</h2>
 * <p>
 * This class performs an inherently dangerous operation: it
 * removes <em>any</em> directory tree. Be very careful how
 * you call this code. As a small security measure, this class
 * provides a global (i.e. static) minimum depth from root parameter
 * that is initialized to 3.
 * </p>
 * <p><small><i>
 * From the Skwish project at sourceforge.
 * </i></small></p>
 */
public class DirectoryRemover {

  private static volatile int minDepthFromRoot = 3;

  public static int getMinDepthFromRoot() {
    return minDepthFromRoot;
  }

  public static void setMinDepthFromRoot(int minDepth) {
    if (minDepth < 1)
      throw new IllegalArgumentException("minDepth = " + minDepth + " < 1");
    minDepthFromRoot = minDepth;
  }


  /**
   * Remove the given file or directory and returns the number of files deleted. The directory
   * structure is traversed and deleted in post-order.
   * 
   * @param root file or directory
   * @return the number of deleted files and directories; negative, if an error was encountered
   *  on the last delete.
   */
  public static int removeTree(File root) {
    if (!root.exists())
      return 0;
    else if (root.isFile())
      return root.delete() ? 1 : -1;
    
    try {
      root = root.getCanonicalFile();
    } catch (IOException iox) {
      return 0;
    }
    int dirDepth = 0;
    File d = root;
    while (dirDepth < minDepthFromRoot) {
      d = d.getParentFile();
      if (d == null)
        break;
      ++dirDepth;
    }
    if (dirDepth < minDepthFromRoot)
      throw new IllegalArgumentException(
          "directory depth (" + dirDepth + ") for " + root +
          " is less than minDepthFromRoot (" +
          minDepthFromRoot + ")");
    Deleter deleter = new Deleter();
    try {
      FileSystemTraverser traverser = new FileSystemTraverser(root);
      traverser.setListener(deleter);
      traverser.run();
    } catch (FailedToDelete dx) {
      return -deleter.count;
    }
    return deleter.count;
  }


  protected static class Deleter implements TraverseListener<File> {
    
    private int count = 0;

    @Override
    public void postorder(File node) {
      boolean deleted = node.delete();
      ++count;
      if (!deleted)
        throw new FailedToDelete(node);
    }

    @Override
    public void preorder(File node) {
    }
    

  }


  @SuppressWarnings("serial")
  private static class FailedToDelete extends IllegalStateException {
    private final File file;
    FailedToDelete(File file) { this.file = file; }
    @Override
    public String getMessage() {
      return "failed to delete " + file;
    }
  }

}

