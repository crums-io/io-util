/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.order;

import java.nio.ByteBuffer;
import java.util.Comparator;

import com.gnahraf.util.ClassEquivalent;

/**
 * Defines an ordering over rows.
 * <p/>
 * <h3>Importance of Equality Semantics</h3>
 * 
 * Elsewhere the code makes a best effort to guard against mixing incompatible row orderings.
 * This compatibility is tested with the <tt>RowOrder.equals(..)</tt> method. It's important,
 * therefore, that subclasses have reasonable equality semantics. The default behavior by
 * the base class is to treat all instances of the same class equivalent.
 * 
 * @see RowOrders
 * @author Babak
 */
public abstract class RowOrder extends ClassEquivalent implements Comparator<ByteBuffer> {
  
  /**
   * Compares two rows. The two rows are assumed to be non null. This is a read-only
   * operation: the implementation shall not ever modify the state
   * of its input arguments; modifying then restoring the position and limits is not
   * permissible.
   * <p/>
   * Note, one of the input arguments may be a key, in which case the implementation
   * may allow the key to have a different length than the row. 
   * 
   * @return the return value has the same semantics as
   *         {@linkplain Comparator#compare(Object, Object)}
   */
  public abstract int compareRows(ByteBuffer rowA, ByteBuffer rowB);

  /**
   * Synonym for {@linkplain #compareRows(ByteBuffer, ByteBuffer)}.
   */
  @Override
  public final int compare(ByteBuffer rowA, ByteBuffer rowB) {
    return compareRows(rowA, rowB);
  }
  
  /**
   * Indicates whether the implementation considers an input {@linkplain ByteBuffer}'s
   * position (and possibly limit) when comparing it to another.
   * 
   * @return <tt>true</tt>, if an input buffer's position matters in comparisons
   */
  public abstract boolean isRelative();
  
  /**
   * The opposite of <tt>isRelative()</tt>.
   */
  public final boolean isAbsolute() {
    return !isRelative();
  }

}
