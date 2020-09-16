/*
 * Copyright 2019 Babak Farhang
 */
package io.crums.util;


import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * 
 */
public class Sets {

  private Sets() {  }
  
  
  
  public static <U, V> Set<V> map(Set<U> source, Isomorphism<U, V> iso) {
    return new SetView<>(source, iso);
  }
  
  
  
  
  
  
  
  protected static class SetView<U, V> extends AbstractSet<V> {
    
    
    private final Set<U> source;
    private final Isomorphism<U, V> iso;
    
    
    
    protected SetView(Set<U> source, Isomorphism<U, V> iso) {
      this.source = Objects.requireNonNull(source, "source");
      this.iso = Objects.requireNonNull(iso, "iso");
    }

    @Override
    public int size() {
      return source.size();
    }

    @Override
    public boolean isEmpty() {
      return source.isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(Object o) {
      if (!iso.isTargetType(o))
        return false;
      
      return source.contains(iso.inverse().apply((V) o));
    }

    @Override
    public Iterator<V> iterator() {
      return Iterators.map(source.iterator(), iso.mapping());
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      for (Object o : c)
        if (!contains(o))
          return false;
      return true;
    }
    
  }

}
