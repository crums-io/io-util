/*
 * Copyright 2019 Babak Farhang
 */
package io.crums.util;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;

/**
 * Iterator utility methods.
 */
public class Iterators {
  
  private Iterators() {  }
  
  
  /**
   * Maps an iterator of one type to another type. The view is read only.
   */
  public static <U, V> Iterator<V> map(Iterator<U> source, Function<U, V> mapper) {
    return new IteratorView<>(source, mapper);
  }
  
  
  public static <T> Iterator<T> limit(Iterator<T> iter, int count) {
    final int[] countDown = new int[] { count };
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return countDown[0] > 0 && iter.hasNext();
      }
      @Override
      public T next() {
        if (--countDown[0] < 0)
          throw new NoSuchElementException();
        return iter.next();
      }
    };
  }
  

  /**
   * Lazily merges and returns 2 iterators as one. Which element comes next is determined by the
   * ordering. If the 2 iterators are already sorted with respect to this order
   * then merged order will also be sorted.
   */
  public static <T extends Comparable<T>> Iterator<T> mergeOrdered(Iterator<T> a, Iterator<T> b) {
    return merge(a, b, Comparator.naturalOrder());
  }
  
  
  /**
   * Lazily merges and returns 2 iterators as one. Which element comes next is determined by the
   * ordering. If the 2 iterators are already sorted with respect to this order
   * then merged order will also be sorted.
   */
  public static <T> Iterator<T> merge(Iterator<T> a, Iterator<T> b, Comparator<T> comparator) {
    return new MergeIterator<>(a, b, comparator);
  }
  
  
  /**
   * Returns an iterator that is the concatentation of the given two iterators.
   */
  public static <T> Iterator<T> concat(Iterator<T> first, Iterator<T> second) {
    Objects.requireNonNull(first, "null first iterator");
    Objects.requireNonNull(second, "null second iterator");
    
    return new Iterator<T>() {

      @Override
      public boolean hasNext() {
        return first.hasNext() || second.hasNext();
      }

      @Override
      public T next() {
        return first.hasNext() ? first.next() : second.next();
      }
    };
  }

  
  static class IteratorView<U, V> implements Iterator<V> {
    
    private final Iterator<U> source;
    private final Function<U, V> mapper;
    
    IteratorView(Iterator<U> source, Function<U, V> mapper) {
      this.source = Objects.requireNonNull(source, "source");
      this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    public boolean hasNext() {
      return source.hasNext();
    }

    @Override
    public V next() {
      return mapper.apply(source.next());
    }
  }
  
  
  
  static class MergeIterator<T> implements Iterator<T> {

    private final Iterator<T> a;
    private final Iterator<T> b;
    private final Comparator<T> comparator;
    
    private T nextA;
    private T nextB;
    
    MergeIterator(Iterator<T> a, Iterator<T> b, Comparator<T> comparator) {
      this.a = Objects.requireNonNull(a, "null iterator a");
      this.b = Objects.requireNonNull(b, "null iterator b");
      this.comparator = comparator;
      this.nextA = next(a);
      this.nextB = next(b);
    }

    private T next(Iterator<T> iter) {
      return iter.hasNext() ? iter.next() : null;
    }
    
    private T nextA() {
      if (nextA == null)
        throw new NoSuchElementException();
      T out = nextA;
      nextA = next(a);
      return out;
    }
    
    private T nextB() {
      if (nextB == null)
        throw new NoSuchElementException();
      T out = nextB;
      nextB = next(b);
      return out;
    }
    
    @Override
    public boolean hasNext() {
      return nextA != null || nextB != null;
    }

    @Override
    public T next() {
      T next;
      if (nextA == null) {
        next = nextB();
      } else if (nextB == null) {
        next = nextA();
      } else if (comparator.compare(nextA, nextB) > 0) {
        next = nextB();
      } else {
        next = nextA();
      }
      return next;
    }
    
    
    
  }
  
  
}








