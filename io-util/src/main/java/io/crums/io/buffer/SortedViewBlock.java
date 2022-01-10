/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Arrays;
import java.util.Comparator;

import io.crums.io.channels.ChannelUtils;

/**
 * This class takes presents a sorted view of of an unsorted block.
 * 
 * @author Babak
 */
public class SortedViewBlock extends SortedBlock {


  public SortedViewBlock(ByteBuffer block, int cellByteWidth, Comparator<ByteBuffer> order) {
    super(block, cellByteWidth, order);
    sortCells();
  }


  public SortedViewBlock(ByteBuffer block, int cellByteWidth, Comparator<ByteBuffer> order, boolean readOnlyCells) {
    super(block, cellByteWidth, order, readOnlyCells);
    sortCells();
  }
  
  
  public void sortCells() {
    Arrays.sort(this.cells, this.order());
  }
  
  
  public void writeSortedCells(GatheringByteChannel file) throws IOException {
    synchronized (cells) {
      try {
        ChannelUtils.writeRemaining(file, cells);
      } finally {
        BufferUtils.clearAll(cells);
      }
    }
  }

}
