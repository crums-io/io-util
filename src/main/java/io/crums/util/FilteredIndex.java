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
 * Concurrent read access is safe so long as the input filter list
 * at {@linkplain #FilteredIndex(List) construction} is not concurrently
 * modified behind the scenes.
 * </p>
 */
public class FilteredIndex {
  
  protected final List<Long> filter;

  /**
   * Constructs a new instance with the given <em>ordered</em> list
   * of unique, non-negative, filtered index numbers. If the given list
   * is immutable, then the new object will also be immutable.
   * <p>
   * Note for performance reasons, the base constructor does not validate.
   * <em>The behavior of this class is undefined if the input list violates the above
   * constraints.</em>
   * 
   * </p>
   * 
   * @param filter non-null, non-negative, ordered list of numbers
   */
  public FilteredIndex(List<Long> filter) {
    this.filter = Objects.requireNonNull(filter, "null filter list");
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
    
    int filteredIndicesAhead = filteredIndicesAhead(filteredIndex);
    
    if (filteredIndicesAhead == 0)
      return filteredIndex;
    
    long unFilteredIndex = filteredIndex + filteredIndicesAhead;

    final int fsize = filter.size();

    assert filteredIndicesAhead <= fsize;
    
    while (filteredIndicesAhead < fsize) {
      int index = filteredIndicesAhead;
      long nextFilter = filter.get(index);
      if (nextFilter > unFilteredIndex)
        break;
      ++filteredIndicesAhead;
      ++unFilteredIndex;
    }
    
    return unFilteredIndex;
  }
  
  
  /**
   * Returns the number of filtered indices ahead of (and including)
   * the given index.
   * 
   * @param index &ge; 0
   * 
   * @return a non-negative number
   */
  public final int filteredIndicesAhead(long index) {
    int fIndex = Collections.binarySearch(filter, index);
    return fIndex < 0 ? -1 - fIndex : fIndex + 1;
  }

}







