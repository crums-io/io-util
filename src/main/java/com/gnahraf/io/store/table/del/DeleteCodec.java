/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.del;


import java.nio.ByteBuffer;

import com.gnahraf.io.store.table.TableSet;

/**
 * Abstract representation of a deleted row. This figures in a {@linkplain TableSet} that supports deleting
 * rows: in order to represent a <em>deletion entry</em> in a table that overrides (masks) another table
 * that contains the to-be-deleted row, we concoct this abstraction. In particular, we don't want to
 * stipulate ahead of time how a deleted row is encoded (for example, by using a special byte in the row): there
 * should be many occasions where a certain value in a row would naturally represent a non-entry as a deleted entry.
 * 
 * @author Babak
 */
public abstract class DeleteCodec {
  
  /**
   * Tests whether the given <tt>row</tt> encodes the deletion of the row.
   */
  public abstract boolean isDeleted(ByteBuffer row);
  
  /**
   * Encodes a deletion marker in the given <tt>row</tt>. An implementation must be careful not to modify the
   * <em>key</em> part[s] of the row. Put another way, the modification must not affect the rank of a row
   * with respect to the table's row order.
   */
  public abstract void markDeleted(ByteBuffer row);

}
