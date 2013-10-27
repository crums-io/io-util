/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.gnahraf.io.store.NotSortedException;
import com.gnahraf.io.store.table.SortedTable.Searcher;
import com.gnahraf.io.store.table.merge.PrecedenceMergeSource;
import com.gnahraf.io.store.table.order.RowOrder;

/**
 * 
 * @author Babak
 */
public class TableSetIterator {
  
  protected final ArrayList<PrecedenceMergeSource> sources;
  
  private final int rowWidth;



  public TableSetIterator(TableSet tableSet, ByteBuffer key) throws IOException {
    if (tableSet == null)
      throw new IllegalArgumentException("null tableSet");
    List<SortedTable> stack = tableSet.tables();
    this.sources = new ArrayList<>(stack.size());
    for (int i = 0; i < stack.size(); ++i) {
      PrecedenceMergeSource source = new PrecedenceMergeSource(newSearcher(stack, i), i);
      Searcher searcher = source.searcher();
      long rowNum;
      if (searcher.search(key))
        rowNum = searcher.getHitRowNumber();
      else
        rowNum = -searcher.getHitRowNumber() - 1;
      source.setRow(rowNum);
      if (!source.finished())
        sources.add(source);
    }
    
    Collections.sort(sources);
    
    this.rowWidth = tableSet.getRowWidth();
  }
  

  private final static int SEARCH_BUFFER_ROWS = 128;
  
  protected Searcher newSearcher(List<SortedTable> stack, int index) throws IOException {
    return stack.get(index).newSearcher(SEARCH_BUFFER_ROWS);
  }
  


  public final int getRowWidth() {
    return rowWidth;
  }
  
  public ByteBuffer next() throws IOException {
    return nextImpl(ByteBuffer.allocate(rowWidth));
  }

  
  public ByteBuffer next(ByteBuffer buffer) throws IOException {
    if (buffer == null)
      throw new IllegalArgumentException("null buffer");
    if (buffer.capacity() < rowWidth)
      throw new IllegalArgumentException("buffer too small: " + buffer);
    return nextImpl(buffer);
  }
  
  
  protected ByteBuffer nextImpl(ByteBuffer next) throws IOException {
    // invariant: sources is sorted and none of its elements is finished
    if (sources.isEmpty())
      return null;

    int index = sources.size() - 1;
    PrecedenceMergeSource top = sources.get(index);
    
    // copy the current top row into *next*
    next.clear();
    next.put(top.row());
    next.flip();
    
    // advance the row number of the top row, and if finished, discard top
    if (!top.advanceRow())
      sources.remove(index);
    
    // now make sure none of the lower precedence sources is positioned at a
    // row that was overridden by top..
    
    RowOrder order = top.searcher().getTable().order();
    
    // work backwards (in order of decreasing source precedence)
    // and skip over source rows that compare equal. the 1st source
    // that compares greater than to-be-returned next row, breaks the
    // iteration..
    while (index-- > 0) {
      // index >= 0
      int comp = order.compareRows(next, sources.get(index).row());
      
      if (comp < 0)
        break;
      
      // sanity check
      if (comp > 0)
        // TODO: more detailed logging by consistently overriding toString()
        throw new NotSortedException("assertion failed. this: " + this);
      
      // comp == 0
      // advance the row number of this source, and if finished, discard it
      if (!sources.get(index).advanceRow())
        // we're iterating backward, so the following remove
        // doesn't affect subsequent lower indices
        sources.remove(index);
    }
    // maintain the post-condition invariant
    Collections.sort(sources);
    return next;
    
  }

}
