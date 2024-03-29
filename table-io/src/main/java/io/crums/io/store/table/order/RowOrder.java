/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.order;

import java.nio.ByteBuffer;
import java.util.Comparator;

import io.crums.util.ClassEquivalent;

/**
 * Defines an ordering over rows. An instance typically does not consider the
 * entire contents of any given row ({@linkplain ByteBuffer}) it evaluates; rather,
 * the <em>examined region</em> of a row (which need not be contiguous) represents the
 * row's <em>key</em> (or unique identifier), while the <em>unexamined region</em> of
 * the row represents its changeable <em>value</em>.
 * 
 * <h2>Importance of Equality Semantics</h2>
 * 
 * <p>
 * Elsewhere the code makes a best effort to guard against mixing incompatible row orderings.
 * This compatibility is tested with the <code>RowOrder.equals(..)</code> method. It's important,
 * therefore, that subclasses have reasonable equality semantics. The default behavior by
 * the base class is to treat all instances of the same class equivalent.
 * </p><p>
 * Because the parent class, if a subclass <em>only</em> has a default constructor, then the correct
 * semantics are already implemented.
 * </p>
 * 
 * 
 * @see RowOrders
 */
public abstract class RowOrder extends ClassEquivalent implements Comparator<ByteBuffer> {
  
  /**
   * Compares two rows. The two rows are assumed to be non null. This is a read-only
   * operation: the implementation shall not ever modify the state
   * of its input arguments; modifying then restoring the position and limits is not
   * permissible.
   * <p>
   * Note, one of the input arguments may be a key, in which case the implementation
   * may allow the key to have a different length than the row.
   * </p>
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

}
