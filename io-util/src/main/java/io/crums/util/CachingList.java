/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util;


import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;

/**
 * An object caching list for layering on top of expensive, "item-on-demand" lists.
 * 
 * <h3>Motivation</h3>
 * <p>
 * We often use lazily loaded views of database objects as ordered (sorted) lists. These can be
 * useful abstractions, particularly when the list won't otherwise fit in memory. For such lists, it
 * would be nice to layer a caching layer so that for when, for example, you binary search for the index
 * of an item  (as with {@linkplain Collections#binarySearch(List, Object)}, and then retrieve that item
 * by index, you don't incur another I/O penalty.
 * </p>
 * 
 * TODO: move me to io-utils project
 * 
 * @see #cach(List, int, double)
 */
public class CachingList<T> extends AbstractList<T> implements RandomAccess {
  
  /**
   * Minimum cache size.
   */
  public final static int MIN_CACHE_SIZE = 2;
  
  public final static int DEFAULT_CACHE_SIZE = 16;
  
  /**
   * Maximum cache size. A sanity check really.
   */
  public final static int MAX_CACHE_SIZE = 64 * 1024;
  
  
  public final static double DEFAULT_TAIL_REFRESH_FACTOR = 0.25;
  
  
  
  /**
   * Returns a caching instance if the <tt>source</tt> is big enough.
   * 
   * @param <T>
   * @param source the underlying list
   * @return {@linkplain #cache(List, int, double) cache(source, DEFAULT_CACHE_SIZE, DEFAULT_TAIL_REFRESH_FACTOR)}
   * 
   * @see #DEFAULT_CACHE_SIZE
   * @see #DEFAULT_TAIL_REFRESH_FACTOR
   */
  public static <T> List<T> cache(List<T> source) {
    return cache(source, DEFAULT_CACHE_SIZE, DEFAULT_TAIL_REFRESH_FACTOR);
  }
  
  /**
   * Returns a caching instance if the <tt>source</tt> is big enough. If the source is empty it is
   * returned as is. If <tt>source</tt> has only one element, then that element is pre-fetched and a
   * read-only singleton list is returned. Otherwise, an instance of this class is constructed and
   * returned.
   * 
   * @param <T>
   * @param source the underlying list
   * @param maxCacheSize the maximum cache size &ge; 2 (the actual cache size might be lowered if <tt>source.size()</tt>
   *    is less than this)
   * @param tailRefreshFactor &gt; 0 and &lt; 1
   * 
   * @return a view of the <tt>source</tt> list
   */
  public static <T> List<T> cache(List<T> source, int maxCacheSize, double tailRefreshFactor) {
    Objects.requireNonNull(source, "null source");
    if (source.isEmpty())
      return source;
    
    if (source.size() == 1)
      return Collections.singletonList(source.get(0));
    
    if (tailRefreshFactor < 0 || tailRefreshFactor >= 1)
      throw new IllegalArgumentException("tailRefreshFactor " + tailRefreshFactor);
    if (maxCacheSize < MIN_CACHE_SIZE)
      throw new IllegalArgumentException("minCacheSize " + maxCacheSize);
    
    int cacheSize = Math.max(MIN_CACHE_SIZE, Math.min(source.size(), maxCacheSize));
    
    int tailRefreshLength = cacheSize >= source.size() ? 0 : (int) (cacheSize * tailRefreshFactor);
    
    return new CachingList<>(source, cacheSize, tailRefreshLength);
  }
  
  
  private final List<T> source;
  
  private final Object[] cachedItems;
  private final int[] cachedIndexes;
  private final int tailRefreshLength;
  
  private int cacheZeroIndex;


  /**
   * Creates an instance with default cache size of 16 and 0 tail refresh length.
   * 
   * @param source the underlying list
   */
  public CachingList(List<T> source) {
    this(source, Math.max(MIN_CACHE_SIZE, Math.min(DEFAULT_CACHE_SIZE, source.size())));
  }

  /**
   * Creates an instance with the given cache size and 0 tail refresh length.
   * 
   * @param source the underlying list
   * @param cacheSize the maximum number of items cached (&gt; 2)
   */
  public CachingList(List<T> source, int cacheSize) {
    this(source, cacheSize, 0);
  }
  
  /**
   * No defaults constructor.
   * 
   * @param source the underlying list
   * @param cacheSize the maximum number of items cached (&gt; 2)
   * @param tailRefreshLength &ge; 0 and &lt; <tt>cacheSize</tt>
   */
  public CachingList(List<T> source, int cacheSize, int tailRefreshLength) {
    this.source = Objects.requireNonNull(source, "null source");
    if (cacheSize < MIN_CACHE_SIZE)
      throw new IllegalArgumentException("cacheSize " + cacheSize + " < " + MIN_CACHE_SIZE);
    if (tailRefreshLength < 0 || tailRefreshLength >= cacheSize)
      throw new IllegalArgumentException("tailRefreshLength " + tailRefreshLength);
    
    this.cachedItems = new Object[cacheSize];
    this.cachedIndexes = new int[cacheSize];
    for (int i = cacheSize; i-- > 0; )
      cachedIndexes[i] = -1;
    
    this.tailRefreshLength = tailRefreshLength;
  }

  
  
  
  @SuppressWarnings("unchecked")
  @Override
  public T get(int index) {
    
    final Object lock = lock();
    final int len = cachedIndexes.length;
    
    synchronized (lock) {
      
      T item = null;
      
      for (int  i = cacheZeroIndex, count = len; count-- > 0; i = (i + 1) % len) {
        
        if (cachedIndexes[i] == index) {
          
          item = (T) cachedItems[i];
          
          if (count > tailRefreshLength)
            return item;
          else
            break;
        }
      } // for (
      
      if (item == null)
        item = source.get(index);
    
      if (cacheZeroIndex == 0)
        cacheZeroIndex = len - 1;
      else
        --cacheZeroIndex;
      
      cachedIndexes[cacheZeroIndex] = index;
      cachedItems[cacheZeroIndex] = item;
      
      return item;
    }
    
    
  }

  
  
  @Override
  public int size() {
    return source.size();
  }
  
  
  private Object lock() {
    return cachedIndexes;
  }

}
