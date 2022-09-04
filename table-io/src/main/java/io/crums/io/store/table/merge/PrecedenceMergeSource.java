/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.merge;

import java.io.IOException;

import io.crums.io.store.table.SortedTable.Searcher;

/**
 * A merge source with precedence. Instances' precedences are used as tie
 * breakers for when they would otherwise compare equal to one another.
 * <p>
 * The use case for this structure is a stack of tables with the the top
 * tables overriding the bottom ones. I.e. if we're merging the underlying tables
 * of a {@linkplain io.crums.io.store.table.TableSet}.
 * </p>
 */
public class PrecedenceMergeSource extends BaseMergeSource<PrecedenceMergeSource> {
  
  private final int precedence;

  public PrecedenceMergeSource(Searcher searcher, int precedence) throws IOException {
    super(searcher);
    this.precedence = precedence;
  }
  
  /**
   * Returns this instance's precedence.
   */
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
    return comp == 0 ? this.precedence -  other.precedence : comp;
  }

}
