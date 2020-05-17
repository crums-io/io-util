/*
 * Copyright 2009-2020 Babak Farhang
 */
package com.gnahraf.io;


import java.io.File;
import java.io.IOException;

import com.gnahraf.util.tree.TraverseListener;



/**
 * Recursively removes the contents of a directory tree.
 * Similar to <tt>rm myDir -rf</tt>.
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


  public static boolean removeTree(File root) {
    try {
      root = root.getCanonicalFile();
    } catch (IOException iox) {
      return false;
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
    try {
      FileSystemTraverser traverser = new FileSystemTraverser(root);
      traverser.setListener(new Deleter());
      traverser.run();
    } catch (FailedToDelete dx) {
      return false;
    }
    return true;
  }


  protected static class Deleter implements TraverseListener<File> {

    @Override
    public void postorder(File node) {
      boolean deleted = node.delete();
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

