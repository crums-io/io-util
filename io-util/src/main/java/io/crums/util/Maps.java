/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Map classes and utilities.
 */
public class Maps {
  
  
  
  /**
   * A swappable {@linkplain Map} implementation base class.
   * Designed to be subclassed. The use case is when a {@code Map}
   * sub-type needs to be sometimes insertion-ordered, other times
   * not.
   */
  public static class DelegateMap<K, V> implements Map<K, V> {
    
    
    protected final Map<K, V> delegate;
    
    
    public DelegateMap(Map<K, V> delegate) {
      this.delegate = Objects.requireNonNull(delegate, "null delegate Map");
    }
    
    
    @Override
    public V put(K key, V value) {
      return delegate.put(key, value);
    }
    
    
    @Override
    public V get(Object key) {
      return delegate.get(key);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
      return delegate.entrySet();
    }


    @Override
    public int size() {
      return delegate.size();
    }


    @Override
    public boolean isEmpty() {
      return delegate.isEmpty();
    }


    @Override
    public boolean containsKey(Object key) {
      return delegate.containsKey(key);
    }


    @Override
    public boolean containsValue(Object value) {
      return delegate.containsValue(value);
    }


    @Override
    public V remove(Object key) {
      return delegate.remove(key);
    }


    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
      delegate.putAll(m);
    }


    @Override
    public void clear() {
      delegate.clear();
    }


    @Override
    public Set<K> keySet() {
      return delegate.keySet();
    }


    @Override
    public Collection<V> values() {
      return delegate.values();
    }


    /**
     * <p>A rare case where we can (and should) also delegate this method.</p>
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
      return delegate.hashCode();
    }


    /**
     * <p>A rare case where we can (and should) also delegate this method.</p>
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
      return delegate.equals(obj);
    }
    
  }
  
  
  
  

}
