/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.iter;


import static io.crums.io.store.table.iter.Direction.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.crums.io.store.table.SortedTable.Searcher;
import io.crums.io.store.table.merge.BaseMergeSource;
import io.crums.io.store.table.merge.PrecedenceMergeSource;

/**
 * 
 * @author Babak
 */
public class DirectionalMergeSource extends PrecedenceMergeSource {



  private Direction direction = FORWARD;

  /**
   * 
   */
  public DirectionalMergeSource(Searcher searcher, int precedence) throws IOException {
    super(searcher, precedence);
  }
  
  


  public final Direction getDirection() {
    return direction;
  }

  
  public final void setDirection(Direction direction) {
    if (direction == null)
      throw new IllegalArgumentException("null direction");
    this.direction = direction;
  }



  /**
   * Instance comparison is reversed if {@linkplain #getDirection() direction}
   * is <code>REVERSE</code>. Note, however, the effect of {@linkplain #precedence()}
   * on tie-breaker comparisons is <em>not</em> reversed.
   */
  @Override
  protected int compareToImpl(BaseMergeSource<?> other) {
    return direction.effectiveComp(super.compareToImpl(other));
  }



  /**
   * Row comparison is reversed if {@linkplain #getDirection() direction}
   * is <code>REVERSE</code>.
   */
  @Override
  public int compareRowWithOther(ByteBuffer otherRow) {
    return direction.effectiveComp(super.compareRowWithOther(otherRow));
  }

  /**
   * The semantics of {@linkplain BaseMergeSource#advanceRow()} and
   * {@linkplain BaseMergeSource#rewindRow()} are reversed iff the
   * {@linkplain #getDirection() direction} is <code>REVERSE</code>.
   * 
   * @return <code>direction == FORWARD ? super.advanceRow() : super.rewindRow()</code>
   */
  @Override
  public boolean advanceRow() throws IOException {
    return direction == FORWARD ? super.advanceRow() : super.rewindRow();
  }


  /**
   * The semantics of {@linkplain BaseMergeSource#advanceRow()} and
   * {@linkplain BaseMergeSource#rewindRow()} are reversed iff the
   * {@linkplain #getDirection() direction} is <code>REVERSE</code>.
   * 
   * @return <code>direction == FORWARD ? super.rewindRow() : super.advanceRow()</code>
   */
  @Override
  public boolean rewindRow() throws IOException {
    return direction == FORWARD ? super.rewindRow() : super.advanceRow();
  }

  
  /**
   * Sets the current row to one matching the given <code>key</code> row, if found;
   * otherwise the current row is set to one immediately following the given
   * key, where <em>following</em> means
   * <ul>
   * <li>the smallest row that compares greater than <code>key</code> if the
   * {@linkplain #getDirection() direction} is <code>FORWARD</code>, or</li>
   * <li>the greatest row that compares less than <code>key</code> if the
   * {@linkplain #getDirection() direction} is <code>REVERSE</code>.</li>
   * </ul>
   * If <code>key</code> is beyond the table range in either direction, then the
   * contents of the current {@linkplain #row() row} will be undefined, and
   * the current {@linkplain #rowNumber() row number} will be <code>-1</code> or
   * {@linkplain #rowCount() rowCount()}. I.e. the instance will be {@linkplain #finished()}.
   * <p>
   * The return value indicates whether the contents of the current row is well
   * defined.
   * </p>
   * 
   * @param key
   *        the look up key
   * @return not {@linkplain #finished()}
   */
  public boolean setRow(ByteBuffer key) throws IOException {
    long rowNum;
    if (searcher().search(key))
      rowNum = searcher().getHitRowNumber();
    else {
      rowNum = -searcher().getHitRowNumber() - 1;
      if (direction == REVERSE)
        rowNum -= 1;
    }
    setRow(rowNum);
    return !finished();
  }

}
