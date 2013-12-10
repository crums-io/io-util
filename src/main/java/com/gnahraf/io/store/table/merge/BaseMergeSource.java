/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.merge;


import java.io.IOException;
import java.nio.ByteBuffer;

import com.gnahraf.io.store.table.SortedTable;
import com.gnahraf.io.store.table.SortedTable.Searcher;

/**
 * We maintain a stack of these in a multiway merge. A merge source encapsulates progress
 * from a single table. These are mutually comparable in a way that allows
 * us to easily track rows from which source table must be copied to the
 * target of a merge.
 * <p/>
 * Here's the basic idea how  works in ASCII art..
 * <p/>
 * Let's say we have 3 sorted tables we want to merge. In order to keep the sketch simple,
 * let's say no 2 rows across any of the tables compare equal.:
 * <pre>
 *   E       B       I
 *   F       C       J
 *   L       G       M
 *           H       P
 *           K       Q
 *           Z       U
 *                   X
 *                   Y
 * </pre>
 * Each column above stands for a sorted table, and each letter in a column stands for the value
 * of a row in that table. A merge source instance is just a wrapper around a table along with a
 * cursor indicating the current (unmerged) row. Here are the same tables, wrapped as merge sources,
 * and sorted in their natural order:
 * <pre>
 *   I<       E<        B<
 *   J        F         C
 *   M        L         G
 *   P                  H
 *   Q                  K
 *   U                  Z
 *   X
 *   Y
 * </pre>
 * Each merge source's cursor is initialized to its table's first row (row number zero).
 * Merge sources are ordered based on the comparing the the row each points to with that
 * of another. The ordering is actually in reverse, in order to more efficiently
 * support tail end removal of merge sources from lists (an optimization, likely not worth the
 * extra cognitive load it introduces). The diagram above exactly depicts this ordering. The
 * tail end of the list of merge sources points to the next row to be written out to the
 * output table. So you can view this ordering of merge sources as a kind of stack with the
 * tail end of the list representing what must be worked on next.
 * <p/>
 * Let's call the tail merge source (far right) in the list the <em>top</em> merge source,
 * and the one immediately to the left of it, the <em>next</em> merge source.
 * <p/>
 * The first step in the merge is to note the index of the <em>next</em>'s row in <em>top</em>.
 * This is the row number in <em>top</em> where where <em>next</em>'s current row would have
 * found itself if it had been solely merged into <em>top</em>'s table. In our example,
 * the index evaluates to 2. Next, all the rows between the <em>top</em>s current row number
 * and the index are block-transfered to the output file.
 * <pre>
 *   I<       E< --     B<                             B
 *   J        F    |    C                              C
 *   M        L     --> G  (row # 2)
 *   P                  H
 *   Q                  K
 *   U                  Z
 *   X
 *   Y
 * </pre>
 * The situation is depicted above after the rows in <em>top</em> have been transfered to
 * output (far right column).
 * <p/>
 * Next the top merge source's cursor (row number) is set to the index noted in the previous
 * step and the merge sources are resorted:
 * <pre>
 *   I<       B         E<                             B
 *   J        C         F                              C
 *   M        G<        L
 *   P        H         
 *   Q        K         
 *   U        Z        
 *   X
 *   Y
 * </pre>
 * <p/>
 * The above 2 steps are repeated until the <em>top</em>'s cursor points beyond its last
 * row. In that event, the top merge source is removed from the list. Processing then resumes
 * until there is but one merge source remaining in the list. The next few steps, are
 * depicted below..
 * <p/>
 * <pre>
 *   I<       B         E<                             B
 *   J        C         F                              C
 *   M        G<      > L                              E
 *   P        H                                        F
 *   Q        K         
 *   U        Z        
 *   X
 *   Y
 * </pre>
 * <p/>
 * <pre>
 *   E        I<        B                              B
 *   F        J         C                              C
 *   L<       M         G<                             E
 *            P         H                              F
 *            Q       > K                              G
 *            U         Z                              H
 *            X
 *            Y
 * </pre>
 * <p/>
 * Finally, when there's only one merge source remaining in the list of merge sources,
 * the remaining rows in that merge source are appended to the output file.
 * 
 * <h3>Don't access concurrently</h3>
 * Designed for single threaded access (not even concurrent reads).
 * <p/>
 * <h4>Note</h4>
 * Don't be daunted by the self-referential type parameter <em><tt>S</tt></em>: it just
 * says that derived instances are mutually comparable.
 * 
 * @author Babak
 */
public abstract class BaseMergeSource<S extends BaseMergeSource<?>> implements Comparable<S> {


  private final Searcher searcher;
  private final long rowCount;
  private final ByteBuffer row;
  private long rowCursor;
  
  public BaseMergeSource(SortedTable.Searcher searcher) throws IOException {
    this(searcher, 0);
  }
  
  public BaseMergeSource(SortedTable.Searcher searcher, long initRowNumber) throws IOException {
    if (searcher == null)
      throw new IllegalArgumentException("null searcher");
    this.searcher = searcher;
    this.rowCount = searcher.getTable().getRowCount();
    if (initRowNumber > rowCount)
      throw new IllegalArgumentException("initRowNumber > rowCount: " + initRowNumber + " > " + rowCount);
    if (rowCount == 0)
      throw new IllegalArgumentException("empty table: " + searcher.getTable());

    this.row = ByteBuffer.allocate(searcher.getTable().getRowWidth());
    setRow(initRowNumber);
  }
  
  
  public final Searcher searcher() {
    return searcher;
  }
  
  public final SortedTable table() {
    return searcher.getTable();
  }
  
  /**
   * Indicates the current row number. If in the range [0, {@linkplain #rowCount() rowCount} - 1],
   * then the contents of that row number is given by {@linkplain #row()}.
   * 
   * @return a number between -1 and {@linkplain #rowCount()}
   */
  public long rowNumber() {
    return rowCursor;
  }
  
  /**
   * Sets the {@linkplain #rowNumber()} and loads the corresponding {@linkplain #row()};
   * unless the <tt>rowNum</tt> argument is equal to either {@linkplain #getRowCount()} or
   * <tt>-1</tt>, in which case the instance is considered finished.
   * 
   * @throws IndexOutOfBoundsException
   */
  public void setRow(long rowNum) throws IOException {
    if (rowNum >= rowCount || rowNum < 0) {
      if (rowNum == rowCount || rowNum == -1) {
        // we're finished
        rowCursor = rowNum;
        return;
      }
      throw new IndexOutOfBoundsException("rowNum/rowCount: " + rowNum + "/" + rowCount);
    }
    row.clear();
    // if the row is already loaded in the search buffer
    if (searcher.isRowInBuffer(rowNum))
      searcher.copyRowInto(rowNum, row);
    // o.w. hit the file system
    else
      searcher.getTable().read(rowNum, row);
    row.flip();
    rowCursor = rowNum;
  }
  
  

  /**
   * Advances to the next row, unless already at the end of this merge source.
   * 
   * @return <tt>true</tt>, if advanced to the next existing row; <tt>false</tt>,
   *         if advanced, or already advanced, past the last row, i.e. if <tt>finished()</tt>.
   */
  public boolean advanceRow() throws IOException {
    if (finished())
      return false;
    setRow(rowNumber() + 1);
    return !finished();
  }

  /**
   * Rewinds to the previous row, unless already at the beginning of this merge source.
   * 
   * @return <tt>true</tt>, if advanced to the previous existing row; <tt>false</tt>,
   *         if rewound, or already rewound, past the first row, i.e. if <tt>finished()</tt>.
   */
  public boolean rewindRow() throws IOException {
    if (finished())
      return false;
    setRow(rowNumber() - 1);
    return !finished();
  }
  
  /**
   * Returns the snapshot row count. (If there are concurrent additions to the
   * underlying table, those will be ignored.)
   * @return
   */
  public final long rowCount() {
    return rowCount;
  }

  
  /**
   * Returns the contents of the current {@linkplain #rowNumber()}, if the
   * instance is not {@linkplain #finished()}; otherwise, the return value is undefined.
   * The returned instance is a new read-only view onto the current row. Its contents will
   * be automatically modified every time the row number is changed.
   */
  public final ByteBuffer row() {
    return row.asReadOnlyBuffer();
  }
  
  /**
   * Copies the contents of the current row into the given <tt>buffer</tt>. Slightly
   * more efficient
   */
  public void copyRowInto(ByteBuffer buffer) {
    // sanity check
    if (row.remaining() != row.capacity())
      throw new IllegalStateException("Concurrent access? current row: " + row);
    buffer.put(row);
    row.rewind();
  }
  
  /**
   * An instance is finished when all its rows have been copied to the target. I.e. when
   * <tt>{@linkplain #rowNumber()} == {@linkplain #rowCount()}.
   */
  public final boolean finished() {
    return rowCursor == rowCount || rowCursor == -1;
  }
  
  
  
  
  

  @Override
  public String toString() {
    StringBuilder string = new StringBuilder(128);
    string.append(getClass().getSimpleName()).append('[');
    appendToStringFields(string).append(']');
    return string.toString();
  }
  
  
  protected StringBuilder appendToStringFields(StringBuilder string) {
    return string.append("table=").append(searcher.getTable()).append(", row=").append(row)
    .append(", rowCount=").append(rowCount).append(", rowNumber=").append(rowNumber());
  }
  
  
  
  protected int compareToImpl(BaseMergeSource<?> other) {
    // finished instances are ordered to the bottom of the stack
    if (this.finished()) {
      return other.finished() ? 0 : -1;
    } else if (other.finished())
      return this.finished() ? 0 : 1;
    // the above is dead code
    else
      return -table().order().compare(this.row, other.row);
  }
  

  
  /**
   * Compares this instance's {@linkplain #row() row} with the <tt>otherRow</tt>.
   * This is functionally equivalent to
   * <pre>
   * <tt>
   * source = .. // a BaseMergeSource
   * source.table().order().compare(source.row(), otherRow)
   * </tt>
   * </pre>
   * except that it's marginally more efficient.
   * 
   */
  public int compareRowWithOther(ByteBuffer otherRow) {
    return table().order().compare(this.row, otherRow);
  }
  
  /**
   * Compares the <tt>otherRow</tt> to this instance's {@linkplain #row() row}.
   * This is functionally equivalent to
   * <pre>
   * <tt>
   * source = .. // a BaseMergeSource
   * source.table().order().compare(otherRow, source.row())
   * </tt>
   * </pre>
   * except that it's marginally more efficient.
   * 
   * @return <tt>-compareRowWithOther(otherRow)</tt>
   */
  public final int compareOtherWithRow(ByteBuffer otherRow) {
    return -compareRowWithOther(otherRow);
  }

}
