/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Map classes and utilities.
 */
public class Maps {
  
  
  
  
  /**
   * Returns a read-only view of the given map with its values converted using
   * the given function.
   * 
   * <h2>Performance Note</h2>
   * <p>
   * Do not use the returned {@code Map}'s {@linkplain Map#entrySet()} for
   * anything other than <em>iterating</em> over the entries; look-up methods
   * on the entry set are slow (linear).
   * </p>
   * 
   * @param <K> the key type
   * @param <U> the base value type
   * @param <V> the target value type
   * 
   * @param valueMapper
   * @return
   */
  public static <K,U,V> Map<K,V> mapValues(Map<K,U> base, Function<U,V> valueMapper) {
    return base.isEmpty() ? Collections.emptyMap() : new MappedValue<>(base, valueMapper);
  }
  
  
  
  /**
   * A read-only view of a {@linkplain Map} whose values are converted to another type via
   * a mapping function. This is meant as a lightweight wrapper for typical {@code Map} operations,
   * <em>however, the {@linkplain #entrySet()} implementation, is particularly inefficient.</em>,
   * 
   * @param <K> the key type
   * @param <U> the base value type
   * @param <V> the target value type
   */
  public static class MappedValue<K, U, V> extends ReadOnlyMap<K, V> {
    
    private final Map<K, U> base;
    private final Function<U, V> valueMapper;
    
    
    /**
     * 
     * @param base          base map
     * @param valueMapper   value-mapping function.
     */
    public MappedValue(Map<K, U> base, Function<U, V> valueMapper) {
      this.base = Objects.requireNonNull(base, "null base");
      this.valueMapper = Objects.requireNonNull(valueMapper, "null valueMapper");
    }
    

    @Override
    public int size() {
      return base.size();
    }

    @Override
    public boolean isEmpty() {
      return base.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
      return base.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
      return value != null && values().contains(value);
    }

    @Override
    public V get(Object key) {
      U baseVal = base.get(key);
      return baseVal == null ? null : valueMapper.apply(baseVal);
    }

    @Override
    public Set<K> keySet() {
      return base.keySet();
    }

    @Override
    public Collection<V> values() {
      return new AbstractCollection<>() {
        @Override
        public Iterator<V> iterator() {
          return Iterators.map(base.values().iterator(), valueMapper);
        }
        @Override
        public int size() {
          return base.size();
        }
      };
    }

    
    /**
     * Do not use unless you simply use the returned object to iterate over the
     * entire entry set. Looks ups in the returned set are super-inefficient (but correct).
     * 
     * @return read-only set
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
      return new AbstractSet<Map.Entry<K,V>>() {
        @Override
        public Iterator<Entry<K, V>> iterator() {
          return Iterators.map(
              base.entrySet().iterator(),
              e -> Map.entry(e.getKey(), valueMapper.apply(e.getValue())));
        }
        @Override
        public int size() {
          return base.size();
        }
      };
    }
    
  }
  
  
  
  public static abstract class ReadOnlyMap<K, V> implements Map<K, V> {
    @Override
    public final V put(K key, V value) {
      throw new UnsupportedOperationException();
    }
    @Override
    public final V remove(Object key) {
      throw new UnsupportedOperationException();
    }
    @Override
    public final void putAll(Map<? extends K, ? extends V> m) {
      throw new UnsupportedOperationException();
    }
    @Override
    public final void clear() {
      throw new UnsupportedOperationException();
    }
    

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (!(o instanceof Map))
        return false;
      Map<?,?> other = (Map<?,?>) o;
      if (size() != other.size())
        return false;
      for (var entry : entrySet()) {
        if (!Objects.equals(entry.getValue(), other.get(entry.getKey())))
          return false;
      }
      return true;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
      return entrySet().hashCode();
    }
    
    
    
  }
  
  

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
