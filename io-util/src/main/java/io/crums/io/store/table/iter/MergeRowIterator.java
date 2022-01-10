/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.iter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * A nexus view over multiple <tt>RowIterator</tt> instances.
 * 
 * @author Babak
 */
public class MergeRowIterator extends RowIterator {
  
  private final List<PrecedenceRowIterator> subs;
  
  private final Direction direction;
  private final int rowWidth;

  
  public MergeRowIterator(List<RowIterator> subs, Comparator<ByteBuffer> rowOrder)
      throws IOException {
    
    if (subs == null || subs.isEmpty())
      throw new IllegalArgumentException("empty subs: " + subs);
    if (rowOrder == null)
      throw new IllegalArgumentException("null rowOrder");
    
    this.subs = new ArrayList<>(subs.size());
    this.direction = subs.get(0).getDirection();
    this.rowWidth = subs.get(0).getRowWidth();
    
    for (int i = 0; i < subs.size(); ++i) {
      PrecedenceRowIterator pIter = new PrecedenceRowIterator(subs.get(i), i, rowOrder);
      if (pIter.getDirection() != direction)
        throw new IllegalArgumentException(
            "direction mismatch at [" + i + "]: expected " + direction + " but actual was " + pIter.getDirection());
      if (pIter.getRowWidth() != rowWidth)
        throw new IllegalArgumentException(
            "rowWidth mistmatch at [" + i + "]: expected " + rowWidth + " but actual was " + pIter.getRowWidth());
      this.subs.add(pIter);
    }
    Collections.sort(this.subs);
  }


  @Override
  public ByteBuffer next() throws IOException {
    if (subs.isEmpty())
      return null;
    
    final PrecedenceRowIterator top = subs.get(0);
    final ByteBuffer next = top.next();
    
    if (!top.hasNext()) {
      subs.remove(0);
      if (subs.isEmpty())
        return next;
    } else if (subs.size() == 1)
      return next;
    
    Collections.sort(subs);
    // skip over the overridden (overwritten at the API level) rows, if any..
    do {
      // invariant: subs is sorted
      PrecedenceRowIterator newTop = subs.get(0);
      
      // compare the about-to-be-returned row with the next row
      // of any sub iterator, and if any match, skip those
      if (top.getRowOrder().compare(next, newTop.peek()) == 0) {
        // skip over the overridden row
        newTop.next();
        
        // sort the iterators (unless we're removing the newTop--in which case,
        // they remain sorted)
        if (newTop.hasNext())
          Collections.sort(subs);
        else
          subs.remove(0);
      } else
        break;
    
    } while (!subs.isEmpty());
    
    return next;
  }


  @Override
  public final Direction getDirection() {
    return direction;
  }


  @Override
  public final int getRowWidth() {
    return rowWidth;
  }

}
