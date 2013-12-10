/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.TreeSet;

import org.junit.Test;

import com.gnahraf.io.buffer.Covenant;
import com.gnahraf.io.store.table.SortedTable.Searcher;
import com.gnahraf.test.TestMethodHarness;
import com.gnahraf.util.MinMaxObserver;

/**
 * 
 * @author Babak
 */
public class SortedTableBuilderTest extends TableSorterTest {

  @Override
  protected void testImpl(int[] values, int rowWidth) throws IOException {
    TreeSet<Integer> expected = new TreeSet<>();
    MinMaxObserver<Integer> minMax = MinMaxObserver.newInstance();
    ByteBuffer work = ByteBuffer.allocate(rowWidth * values.length);
    final int padding = rowWidth - ROW_WIDTH;
    for (int i = values.length; i-- > 0; ) {
      expected.add(values[i]);
      minMax.observe(values[i]);
      work.putInt(values[i]);
      for (int p = padding; p-- > 0;)
        work.put((byte) (p ^ values[i]));
    }
    {
      assertEquals(expected.first(), minMax.min());
      assertEquals(expected.last(), minMax.max());
    }
    work.flip();
    File sortedFile = new File(unitTestDir(), "sorted");
    FileChannel file = new RandomAccessFile(sortedFile, "rw").getChannel();
    SortedTableBuilder builder = new SortedTableBuilder(rowWidth, ORDER);
    beginSortNanos = System.nanoTime();
    builder.putRows(work, Covenant.WONT_MOD);
    builder.flush(file, true);
    endSortNanos = System.nanoTime();
    
    SortedTable sortedTable = new SortedTable(file, 0, rowWidth, ORDER);
    assertEquals(expected.size(), sortedTable.getRowCount());
    
    Searcher searcher = sortedTable.newSearcher(8);
    
    for (int i = Math.min(100, values.length); i-- > 0; ) {
      int exp = values[i];
      work.clear();
      work.putInt(exp).limit(rowWidth).position(0);
      boolean found = searcher.search(work);
      assertTrue(found);
    }
    work.clear().limit(rowWidth);
    sortedTable.read(0, work);
    work.flip();
    assertEquals(minMax.min().intValue(), work.getInt());

    work.clear().limit(rowWidth);
    sortedTable.read(expected.size() - 1, work);
    work.flip();
    assertEquals(minMax.max().intValue(), work.getInt());
    
    sortedTable.close();
  }

}
