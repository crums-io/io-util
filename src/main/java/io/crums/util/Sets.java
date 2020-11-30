/*
 * Copyright 2019 Babak Farhang
 */
package io.crums.util;


import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

/**
 * Utility methods for working sets. Mostly read-only, immutable idioms.
 */
public class Sets {

  private Sets() {  }
  
  
  /**
   * Returns a mapped view of the given <tt>source</tt> set. The elements of the
   * set are lazily constucted.
   * 
   * @param <U> the source type
   * @param <V> the target (return) type
   * 
   * @param source the source set (unmodified)
   * @param iso isomorphism defining reversible mapping
   * @return
   */
  public static <U, V> Set<V> map(Set<U> source, Isomorphism<U, V> iso) {
    return new SetView<>(source, iso);
  }
  
  
  /**
   * Determines whether the given two sets intersect (have one or more common elements).
   */
  public static <T> boolean intersect(SortedSet<T> a, SortedSet<T> b) {
    return firstIntersection(a, b) != null;
  }
  
  
  /**
   * Returns the first element contained in both sets or <tt>null</tt>, if no such
   * element is found.
   */
  @SuppressWarnings("unchecked")
  public static <T> T firstIntersection(SortedSet<T> a, SortedSet<T> b) {
    Objects.requireNonNull(a, "null a");
    Objects.requireNonNull(b, "null b");
    Comparator<? super T> comparator = a.comparator();
    if (!Objects.equals(comparator, b.comparator()))
      throw new IllegalArgumentException("comparators do not match: " + a + " , " + b.comparator());
    
    if (a.isEmpty() || b.isEmpty())
      return null;
    
    while (!a.isEmpty() && !b.isEmpty()) {
      
      T firstA = a.first();
      T firstB = b.first();
      
      int comp = comparator == null ?
          ((Comparable<T>) firstA).compareTo(firstB) :
            comparator.compare(firstA, firstB);
      
      if (comp < 0)
        // firstA < firstB
        a = a.tailSet(firstB);
      else if (comp > 0)
        // firstB < firstA
        b = b.tailSet(firstA);
      else
        return firstA;
    }
    return null;
  }
  

  /**
   * Returns a <tt>SortedSet</tt> view of the given ordered list. The returned object has undefined behavior
   * if the given list contains out-of-order elements.
   * 
   * @param orderedList the backing ordered list
   * 
   * @return non-null
   */
  public static <T extends Comparable<T>> SortedSet<T> sortedSetView(List<T> orderedList) {
    return sortedSetView(orderedList, null);
  }
  
  /**
   * Returns a <tt>SortedSet</tt> view of the given ordered list. The returned object has undefined behavior
   * if the given list contains out-of-order elements, or if the <tt>comparator</tt> is <tt>null</tt> and the
   * elements are not mutually comparable.
   * 
   * @param <T> the type (implements <tt>Comparable&lt;T&gt;</tt> if <tt>comparator</tt> is <tt>null</tt>)
   * 
   * @param orderedList the backing ordered list
   * @param comparator defines the ordering; if <tt>null</tt>, then <tt>&lt;T&gt;</tt> implements
   *                   <tt>Comparable&lt;T&gt;</tt>
   * @return non-null
   */
  public static <T> SortedSet<T> sortedSetView(List<T> orderedList, Comparator<T> comparator) {
    Objects.requireNonNull(orderedList, "null orderedList");
    return orderedList.isEmpty() ? Collections.emptySortedSet() : new SetViewList<>(orderedList, comparator);
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
  
  
  protected static class SetViewList<T> extends AbstractSet<T> implements SortedSet<T> {
    
    private final List<T> elements;
    private final Comparator<T> comparator;
    
    protected SetViewList(List<T> orderedList, Comparator<T> comparator) {
      this.elements = Objects.requireNonNull(orderedList, "null orderedList");
      this.comparator = comparator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T[] toArray(Object[] a) {
      return (T[]) elements.toArray(a);
    }

    @Override
    public boolean add(Object e) throws UnsupportedOperationException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      return elements.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) throws UnsupportedOperationException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) throws UnsupportedOperationException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) throws UnsupportedOperationException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Comparator<T> comparator() {
      return comparator;
    }

    @Override
    public SortedSet<T> subSet(T fromElement, T toElement) {
      int size = elements.size();
      int startIndex = binarySearch(fromElement);
      if (startIndex < 0)
        startIndex = -1 - startIndex;
      
      if (startIndex == size)
        return Collections.emptySortedSet();
      
      int endIndex = binarySearch(toElement);
      if (endIndex < 0)
        endIndex = -1 - endIndex;
      
      if (endIndex <= startIndex)
        return Collections.emptySortedSet();
      
      if (startIndex == 0 && endIndex == size)
        return this;
      
      return new SetViewList<>(elements.subList(startIndex, endIndex), comparator);
    }

    @Override
    public SortedSet<T> headSet(T toElement) {
      int index = binarySearch(toElement);
      if (index < 0)
        index = -1 - index;
      int size = elements.size();
      if (index == size)
        return this;
      
      return index == 0 ?
          Collections.emptySortedSet() :
            new SetViewList<>(elements.subList(0, index), comparator);
    }
    

    @Override
    public SortedSet<T> tailSet(T fromElement) {
      int index = binarySearch(fromElement);
      if (index < 0)
        index = -1 - index;
      if (index == 0)
        return this;
      
      int size = elements.size();
      
      return index == size ?
          Collections.emptySortedSet() :
            new SetViewList<>(elements.subList(index, size), comparator);
    }

    @Override
    public T first() {
      return elements.isEmpty() ? null : elements.get(0);
    }

    @Override
    public T last() {
      int size = elements.size();
      return size == 0 ? null : elements.get(size - 1);
    }

    @Override
    public Iterator<T> iterator() {
      return elements.iterator();
    }

    @Override
    public int size() {
      return elements.size();
    }
    
    
    @SuppressWarnings("unchecked")
    private int binarySearch(T element) {
      return comparator == null ?
          Collections.binarySearch((List<? extends Comparable<T>>) elements, element) :
            Collections.binarySearch(elements, element, comparator);
      }
    
  }
  

}
