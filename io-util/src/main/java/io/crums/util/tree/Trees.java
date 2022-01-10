/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util.tree;

/**
 * A place to document stuff about trees.
 */
public class Trees {
  
  
  /**
   * Returns the breadth-first, serial index of the node in a binary tree
   * with the given coordinates.
   * 
   * @param depth  depth from root (&ge; 0 and &lt; 32)
   * @param indexAtDepth index at that depth ((&ge; 0 and &lt; {@code (1 << depth)})
   * 
   * @return {@code (1 << (depth - 1)) - 1 + indexAtDepth} if {@code depth} &gt; 0; 0, o.w.
   * 
   * @throws IllegalArgumentException if arguments violate above constraints
   * @see #binaryTreeSerialIndexSansCheck(int, int)
   */
  public static int binaryTreeSerialIndex(int depth, int indexAtDepth) {
    if (depth < 0 || depth > 31)
      throw new IllegalArgumentException("depth: " + depth);
    if (indexAtDepth < 0)
      throw new IllegalArgumentException("indexAtDepth: " + indexAtDepth);
    if (indexAtDepth > 1 << depth)
      throw new IllegalArgumentException(
          "indexAtDepth " + indexAtDepth + " is out-of-bounds with depth " + depth);
    
    return binaryTreeSerialIndexSansCheck(depth, indexAtDepth);
  }
  

  /**
   * Returns the breadth-first, serial index of the node in a binary tree
   * with the given coordinates. <em>In the interest of speed, arguments are not
   * checked.</em>
   * 
   * @param depth  depth from root (&ge; 0 and &lt; 32)
   * @param indexAtDepth index at that depth ((&ge; 0 and &lt; {@code (1 << depth)})
   * 
   * @return  if {@code depth} &gt; 0, returns {@code (1 << (depth - 1)) - 1 + indexAtDepth}; 0, o.w.
   * @see #binaryTreeSerialIndex(int, int)
   */
  public static int binaryTreeSerialIndexSansCheck(int depth, int indexAtDepth) {
    if (depth == 0) {
      assert indexAtDepth == 0;
      return 0;
    }
    // number of nodes up to depth - 1
    int countAtStartOfDepth = (1 << (depth - 1)) - 1;
    return countAtStartOfDepth + indexAtDepth;
  }

  
  private Trees() {  }

}
