/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.util;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;

/**
 * A cache for objects indexed by (zero-based) numbers designed to support a
 * very simple notion of <em>locality-of-reference</em>.
 * <h3>Main Idea</h3>
 * <p>
 * This cache specializes in maintaining caches of objects indexed thru a
 * <em>contiguous</em> range of numbers. It's only property of note is that
 * it gracefully handles growing this contiguous range of cached objects
 * from either end of the already cached range; if the cache is full, it drops
 * the lowest (or highest) indexed object and adds a new cached object as the next
 * higher (lower) slot.
 * </p><p>
 * So this cache mostly works efficiently, if its {@linkplain #get(int) get(index)}
 * method is called with consecutive numbers.
 * </p>
 * <h3>Motivation</h3>
 * <p>
 * In GUI applications that have a scrollable view of a very long list of items,
 * maintaining such a cache is helpful. The items themselves may come from a database,
 * or even from lines in a text file. If there a very many items (lines) at the source,
 * it's not feasible to hold the whole list in memory, much less the GUI elements that
 * represent them.
 * </p>
 */
public class CacheWheel<T> {
  
  private final T[] slots;
  private final Function<Integer, T> factory;
  
  /**
   * Slot-zero, rotary index.
   */
  private int szi;
  private int slotsValid;
  private int sz;

  private int maxSize;
  
  public CacheWheel(T[] slots, Function<Integer, T> factory) {
    this(slots, factory, Integer.MAX_VALUE);
  }
  
  public CacheWheel(T[] slots, Function<Integer, T> factory, int maxSize) {
    this.slots = Objects.requireNonNull(slots, "null slots");
    this.factory = Objects.requireNonNull(factory, "null factory");
    setMaxSize(maxSize);
    if (slots.length < 2)
      throw new IllegalArgumentException("too few slots: " + slots.length);
  }
  
  
  
  public T get(int index) {
    Objects.checkIndex(index, maxSize);
    
    final int deltaSz = index - sz;
    
    // if it's a hit, return it
    if (deltaSz >= 0 && deltaSz < slotsValid)
      return slots[(szi + deltaSz) % slots.length];
    
    // generate the output..
    
    // Note if the factory fails here,
    // then the state of the instance is still self consistent
    // (anticipating off-by-1 errors at the source--if maxSize is set incorrectly)
    
    final T out = factory.apply(index);
    
    // update the cache wheel
    if (deltaSz == slotsValid) {
      if (slotsValid < slots.length) {
        ++slotsValid;
        slots[(szi + deltaSz) % slots.length] = out;
      } else {
        // there are no more available slots; roll over
        slots[szi] = out;
        szi = (szi + 1) % slots.length;
        ++sz;
      }
    } else if (deltaSz == -1) {
      szi = (szi + slots.length - 1) % slots.length;
      slots[szi] = out;
      sz = index;
      if (slotsValid < slots.length)
        ++slotsValid;
    } else {
      szi = 0;
      sz = index;
      slots[0] = out;
      slotsValid = 1;
    }
    
    return out;
  }

  
  /**
   * Returns the number of objects in the cache.
   * 
   * @return &ge; 0 (always &gt; 0 after first use of {@linkplain #get(int)}
   */
  public final int getCacheCount() {
    return slotsValid;
  }
  
  
  /**
   * Returns the maximum number of objects cached (ie, the number of slots).
   */
  public final int getCapacity() {
    return slots.length;
  }
  
  
  /**
   * Determines whether the instance is empty. An instance is only ever
   * empty before its {@linkplain #get(int) get(index)} method has been called.
   * 
   * @return {@code getCacheCount() == 0}
   * 
   */
  public final boolean isEmpty( ) {
    return slotsValid == 0;
  }
  
  /**
   * Returns the lowest cached index (inclusive);
   * zero if {@linkplain #isEmpty() empty}.
   */
  public final int getFloorIndex() {
    return sz;
  }
  
  /**
   * Returns the highest cached index (<em>ex</em>clusive).
   */
  public final int getCeilingIndex() {
    return sz + slotsValid;
  }
  
  
  public final int getCenterIndex() {
    return (getFloorIndex() + getCeilingIndex()) / 2;
  }
  


  /**
   * 
   * @param from (inclusive) &ge; 0
   * @param to   (<em>ex</em>clusive) &ge; -1
   * @return
   */
  public Iterator<T> rangeIterator(int from, int to) {
    checkRangeArgs(from, to);
    
    if (from == to)
      return Collections.emptyIterator();
    
    return new Iterator<T>() {
      final int delta = from < to ? 1 : -1;
      int cursor = from;

      @Override
      public boolean hasNext() {
        return cursor != to;
      }

      @Override
      public T next() {
        if (cursor == to)
          throw new NoSuchElementException();
        T out = get(cursor);
        cursor += delta;
        return out;
      }
    };
  }
  
  
  private void checkRangeArgs(int from, int to) {
    if (from < 0)
      throw new IllegalArgumentException("from: " + from);
    if (to < -1)
      throw new IllegalArgumentException("to: " + to);
    if (from >= maxSize)
      throw new IllegalArgumentException("from (" + from + ") >= max size (" + maxSize + ")");
    if (to > maxSize)
      throw new IllegalArgumentException("to (" + to + ") > max size (" + maxSize + ")");
  }
  

  /**
   * Returns a view of this instance as an ordered collection. It's lazy: which
   * means {@linkplain #rangeIterator(int, int) iterating} over it has side effects on
   * the state of the cache.
   * 
   * @param from inclusive: &ge; 0, &lt; {@linkplain #getMaxSize()}
   * @param to   <em>ex</em>clusive: &ge; -1, &le; {@linkplain #getMaxSize()}
   * 
   * @return a <em>lazily</em> loaded view
   */
  public Collection<T> rangeCollection(int from, int to) {
    checkRangeArgs(from, to);
    return
        from == to ?
            Collections.emptyList() :
              
              new AbstractCollection<T>() {
                @Override
                public Iterator<T> iterator() {
                  return rangeIterator(from, to);
                }
            
                @Override
                public int size() {
                  return Math.abs(to - from);
                }
              };
  }
  
  
  
  public void setMaxSize(int maxSize) {
    if (maxSize < 0)
      throw new IllegalArgumentException("negative maxSize: " + maxSize);
    this.maxSize = maxSize;
  }
  
  
  public final int getMaxSize() {
    return maxSize;
  }

}
