/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.io.store.table;


import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.TreeSet;

import io.crums.io.block.Covenant;
import io.crums.io.store.table.SortedTable.Searcher;

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
    @SuppressWarnings("resource")
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
  

  /**
   * Utility for tracking the minimum and maximum of a corpus.
   * 
   * Removed from io-xp lib and copied here,
   * cuz this was its only use case. Not enuf to keep in library
   */
  static abstract class MinMaxObserver<T> {
    
    protected T min;
    protected T max;
    
    
    public void observe(T value) {
      if (value == null)
        return;
      if (min == null) {
        min = max = value;
        return;
      }
      int maxComp = compare(value, max);
      if (maxComp < 0) {
        if (compare(value, min) < 0)
          min = value;
      } else if (maxComp > 0) {
        max = value;
      }
    }
    
    
    protected abstract int compare(T a, T b);
    
    
    /**
     * Returns the minimum observed instance. If multiple instances
     * evaluated equal to the minimum, then the returned instance is
     * the first observed minimum.
     */
    public T min() {
      return min;
    }
    
    /**
     * Returns the maximum observed instance. If multiple instances
     * evaluated equal to the maximum, then the returned instance is
     * the first observed maximum.
     */
    public T max() {
      return max;
    }
    
    /**
     * Clears the instance. {@linkplain #isSet()} returns <tt>false</tt>.
     */
    public void clear() {
      min = max = null;
    }
    
    /**
     * Tells whether any values have been observed.
     */
    public boolean isSet() {
      return min != null;
    }
    
    
    
    public static <T> MinMaxObserver<T> newInstance(Comparator<T> comparator) {
      return new ComparatorImpl<>(comparator);
    }
    
    
    public static <T extends Comparable<T>> MinMaxObserver<T> newInstance() {
      return new ComparableImpl<>();
    }
    
    
    
    
    
    
    protected static class ComparatorImpl<T> extends MinMaxObserver<T> {
      private final Comparator<T> comparator;
      protected ComparatorImpl(Comparator<T> comparator) {
        this.comparator = comparator;
        if (comparator == null)
          throw new IllegalArgumentException("null comparator");
      }
      @Override
      protected int compare(T a, T b) {
        return comparator.compare(a, b);
      }
    }
    
    
    protected static class ComparableImpl<T extends Comparable<T>> extends MinMaxObserver<T> {
      @Override
      protected int compare(T a, T b) {
        return a.compareTo(b);
      }
    }
  
  }


}
