/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.merge;

import java.io.IOException;

import com.gnahraf.io.store.table.SortedTable.Searcher;

/**
 * 
 * @author Babak
 */
public class PrecedenceMergeSource extends BaseMergeSource<PrecedenceMergeSource> {
  
  private final int precedence;

  public PrecedenceMergeSource(Searcher searcher, int precedence) throws IOException {
    super(searcher);
    this.precedence = precedence;
  }
  
  
  public final int precedence() {
    return precedence;
  }


  @Override
  public int compareTo(PrecedenceMergeSource other) {
    // we're removing finished instances from our collections
    // so might as well, assert some sanity here..
    if (this.finished() || other.finished()) {
      throw new IllegalStateException(
          "assertion failure: this/other is finished. this=" + this + "; other=" + other);
    }
    int comp = compareToImpl(other);
    return comp == 0 ? other.precedence -  this.precedence : comp;
  }
  
  
  

}
