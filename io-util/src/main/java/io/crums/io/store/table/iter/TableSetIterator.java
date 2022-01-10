/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table.iter;


import static io.crums.io.store.table.iter.Direction.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.crums.io.store.NotSortedException;
import io.crums.io.store.table.SortedTable;
import io.crums.io.store.table.TableSet;
import io.crums.io.store.table.SortedTable.Searcher;

/**
 * 
 * @author Babak
 */
public class TableSetIterator extends RowIterator {
  
  protected final ArrayList<DirectionalMergeSource> activeSources;
  protected final List<DirectionalMergeSource> sources;
  
  private final int rowWidth;

  private Direction direction = FORWARD;


  public TableSetIterator(TableSet tableSet) throws IOException {
    if (tableSet == null)
      throw new IllegalArgumentException("null tableSet");
    
    List<SortedTable> stack = tableSet.tables();

    {
      ArrayList<DirectionalMergeSource> all = new ArrayList<>(stack.size());
      for (int i = 0; i < stack.size(); ++i)
        all.add(new DirectionalMergeSource(newSearcher(stack, i), i));
      this.sources = Collections.unmodifiableList(all);
    }
    this.activeSources = new ArrayList<>(stack.size());
    this.rowWidth = tableSet.getRowWidth();
  }
  

  private final static int SEARCH_BUFFER_SIZE = 8 * 1024;
  
  protected Searcher newSearcher(List<SortedTable> stack, int index) throws IOException {
    SortedTable table = stack.get(index);
    int bufferRowCount = (int) Math.min(
        table.getRowCount(), SEARCH_BUFFER_SIZE / table.getRowWidth());
    return table.newSearcher(bufferRowCount);
  }
  
  
  public void init(ByteBuffer key, Direction direction) throws IOException {
    if (direction == null)
      throw new IllegalArgumentException("null direction");
    if (key == null || !key.hasRemaining())
      throw new IllegalArgumentException("key: " + key);
    this.direction = direction;
    activeSources.clear();
    for (DirectionalMergeSource source : sources) {
      source.setDirection(direction);
      if (source.setRow(key))
        activeSources.add(source);
    }
    Collections.sort(activeSources);
  }
  


  /**
   * Returns the direction of this iteration.
   * 
   * @see #init(ByteBuffer, Direction)
   */
  public final Direction getDirection() {
    return direction;
  }

  public final int getRowWidth() {
    return rowWidth;
  }

  
  @Override
  public ByteBuffer next() throws IOException {
    return activeSources.isEmpty() ? null : nextImpl(ByteBuffer.allocate(rowWidth));
  }

  
  
  @Override
  public ByteBuffer next(ByteBuffer buffer) throws IOException {
    if (buffer == null)
      throw new IllegalArgumentException("null buffer");
    if (buffer.capacity() < rowWidth)
      throw new IllegalArgumentException("buffer too small: " + buffer);
    return nextImpl(buffer);
  }
  
  
  protected ByteBuffer nextImpl(ByteBuffer out) throws IOException {
    ByteBuffer next = out;
    // invariant: sources is sorted and none of its elements is finished
    if (activeSources.isEmpty())
      return null;

    // the source index
    int index = activeSources.size() - 1;
    
    DirectionalMergeSource top = activeSources.get(index);
    
    // copy the current top row into *next*
    next.clear();
    top.copyRowInto(next);
    next.flip();
    
    // advance the row number of the top row, and if finished, discard top
    if (!top.advanceRow())
      activeSources.remove(index);
    
    // now make sure none of the lower precedence sources is positioned at a
    // row that was overridden by top..
    
    
    // work backwards (in order of decreasing source precedence)
    // and skip over source rows that compare equal. the 1st source's current
    // row that compares greater than to-be-returned next row, breaks the
    // iteration (since we then know that all subsequent sources must
    // also compare greater (by virtue of how MergeSources are ordered)) 
    while (index-- > 0) {
      // index >= 0
      int comp = activeSources.get(index).compareOtherWithRow(next);
      
      if (comp < 0)
        break;
      
      // sanity check
      if (comp > 0)
        // TODO: more detailed logging by consistently overriding toString()
        throw new NotSortedException("assertion failed at index [" + index + "]. this: " + this);
      
      // comp == 0
      // advance the row number of this source, and if finished, discard it
      if (!activeSources.get(index).advanceRow())
        // we're iterating backward, so the following remove
        // doesn't affect the index position of to-be-visited sources
        // at the lower indices
        activeSources.remove(index);
    }
    // maintain the post-condition invariant
    Collections.sort(activeSources);
    return next;
    
  }
  

  
  
  
  

}
