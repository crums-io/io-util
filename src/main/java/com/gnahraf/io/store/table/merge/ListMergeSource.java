/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.merge;


import java.io.IOException;
import com.gnahraf.io.store.table.SortedTable;



/**
 * Merge source allowing duplicates.
 * 
 * @author Babak
 */
public class ListMergeSource extends BaseMergeSource<ListMergeSource> {


  public ListMergeSource(SortedTable.Searcher searcher) throws IOException {
    super(searcher);
  }

  
  /**
   * Instances are sorted in reverse order of their row content. (An instance always
   * has row contents for <em>some</em> row, unless it's {@linkplain #finished()}.
   * 
   * @throws IllegalStateException
   *         if <tt>this</tt> or the <tt>other</tt> is {@linkplain #finished() finished}
   */
  @Override
  public int compareTo(ListMergeSource other) {
    // we're removing finished instances from our collections
    // so might as well, assert some sanity here..
    if (this.finished() || other.finished()) {
      throw new IllegalStateException(
          "assertion failure: this/other is finished. this=" + this + "; other=" + other);
    }
    
    return compareToImpl(other);
  }
  
}
