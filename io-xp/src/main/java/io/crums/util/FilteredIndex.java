/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A filtered domain of indices.
 * 
 * <h3>Motivation</h3>
 * <p>
 * A list or table may sometimes include special bookkeeping rows
 * that are not really part of (or are in some respect orthogonal to)
 * the ordinary user data that the table is composed of. In such cases,
 * an ability to present a "filtered out view" of the table or list
 * can be useful. This class handles the <em>index translation</em>
 * between <em>filtered</em> and <em>unfiltered</em> indices.
 * </p>
 * <h3>Safe Under Concurrent Access</h3>
 * <p>
 * Concurrent read access is safe. Instances are immutable.
 * </p>
 */
public class FilteredIndex {
  
  protected final List<Long> filter;

  /**
   * Constructs a new instance with the given <em>ordered</em> list
   * of unique, non-negative, filtered index numbers.
   * 
   * @param filter non-null, non-negative, ordered list of numbers
   */
  public FilteredIndex(List<Long> filter) {
    this.filter = Lists.readOnlyOrderedCopy(Objects.requireNonNull(filter, "null filter list"));
    if (!filter.isEmpty() && filter.get(0) < 0)
      throw new IllegalArgumentException("negative filter element at index [0]: " + filter.get(0));
  }
  
  
  /**
   * Converts and returns the given <em>unfiltered</em> index as a
   * filtered <b>int</b> index.
   * 
   * @see #toFilteredIndex(long)
   */
  public int toFilteredIndex(int unFilteredIndex) {
    return (int) toFilteredIndex((long) unFilteredIndex);
  }
  
  /**
   * Converts and returns the given <em>unfiltered</em> index as a
   * filtered index. The mapping is <em>many-to-one</em>, i.e. multiple
   * (consecutive) distinct inputs may return the same value.
   * 
   * @param unFilteredIndex &ge; 0
   * 
   * @return &ge; -1 and &le; {@code unFilteredIndex}: if every index at
   * or below {@code unFilteredIndex} is a filtered one, then -1 is returned.
   * Otherwise, the return value is non-negative.
   * 
   * @see #toUnfilteredIndex(long)
   */
  public final long toFilteredIndex(long unFilteredIndex) {
    if (unFilteredIndex < 0)
      throw new IllegalArgumentException("unFilteredIndex " + unFilteredIndex);
    
    return unFilteredIndex - filteredIndicesAhead(unFilteredIndex);
  }
  
  /**
   * Converts and returns the given <em>filtered</em> index as an
   * unfiltered <b>int</b> index.
   * 
   * @see #toUnfilteredIndex(long)
   */
  public final int toUnfilteredIndex(int filteredIndex) {
    long unfiltered = toUnfilteredIndex((long) filteredIndex);
    if (unfiltered > Integer.MAX_VALUE)
      throw new IllegalArgumentException(
          "filteredIndex " + filteredIndex + " maps beyond int type: " + unfiltered);
    return (int) unfiltered;
  }
  
  /**
   * Converts and returns the given <em>filtered</em> index as an
   * unfiltered index. The mapping is <em>one-to-one</em>.
   * 
   * 
   * 
   * @param filteredIndex &ge; 0
   * 
   * @return &ge; {@code filteredIndex}
   * 
   * @see #toFilteredIndex(long)
   * @see #toUnfilteredIndex(int)
   */
  public final long toUnfilteredIndex(final long filteredIndex) {
    
    if (filteredIndex < 0)
      throw new IllegalArgumentException("filteredIndex " + filteredIndex);
    
    final int maxAhead = this.filter.size();
    
    int aheadCount = filteredIndicesAhead(filteredIndex);
    
    if (aheadCount != 0 && aheadCount != maxAhead) {
      while (aheadCount < maxAhead ) {
        int fAhead = filteredIndicesAhead(filteredIndex + aheadCount);
        if (fAhead == aheadCount)
          break;
        else {
          assert aheadCount < fAhead;
          aheadCount = fAhead;
        }
      }
    }
    
    
    return filteredIndex + aheadCount;
  }
  
  
  
  
  
  /**
   * Returns the number of filtered indices ahead of (and including)
   * the given <em>unfiltered</em> index. Here <em>including</em> means if the {@code index}
   * 
   * @param unfilteredIndex &ge; 0
   * 
   * @return a non-negative number
   */
  public final int filteredIndicesAhead(long unfilteredIndex) {
    int fIndex = Collections.binarySearch(filter, unfilteredIndex);
    return fIndex < 0 ? -1 - fIndex : fIndex + 1;
  }

}







