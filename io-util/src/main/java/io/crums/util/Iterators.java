/*
 * Copyright 2019 Babak Farhang
 */
package io.crums.util;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

/**
 * 
 */
public class Iterators {
  
  private Iterators() {  }
  
  
  /**
   * Maps an iterator of one type to another type. The view is read only.
   */
  public static <U, V> Iterator<V> map(Iterator<U> source, Function<U, V> mapper) {
    return new IteratorView<>(source, mapper);
  }

  
  protected static class IteratorView<U, V> implements Iterator<V> {
    
    private final Iterator<U> source;
    private final Function<U, V> mapper;
    
    protected IteratorView(Iterator<U> source, Function<U, V> mapper) {
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
}
