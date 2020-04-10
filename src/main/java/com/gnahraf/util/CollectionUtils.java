/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.util;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.RandomAccess;
import java.util.Set;

/**
 * 
 * @author Babak
 */
public class CollectionUtils {

  private CollectionUtils() {   }
  
  
  /**
   * Some algos check for the RA marker interface.
   */
  private final static class ReadOnlyList<T> extends AbstractList<T> implements RandomAccess {
    
    private final T[] array;
    
    ReadOnlyList(T[] array) {
      this.array = array;
    }
    
    @Override
    public T get(int index) {
      return array[index];
    }
    @Override
    public int size() {
      return array.length;
    }
  }
  
  
  public static <T> List<T> asReadOnlyList(final T[] array) {
    if (array.length == 0)
      return Collections.emptyList();
    return new ReadOnlyList<T>(array);
  }
  
  
  public static <T> List<T> readOnlyCopy(List<T> orig) {
    if (orig.isEmpty())
      return Collections.emptyList();
    if (orig.size() == 1)
      return Collections.singletonList(orig.get(0));
    ArrayList<T> copy = new ArrayList<>(orig.size());
    copy.addAll(orig);
    return Collections.unmodifiableList(copy);
  }
  
  
  public static boolean intersect(Set<?> a, List<?> b) {
    return intersect(a, (Iterable<?>) b);
  }
  
  
  public static boolean intersect(Collection<?> a, Collection<?> b) {
    if (b.size() > a.size()) {
      Collection<?> c = a;
      a = b;
      b = c;
    }
    return intersect(a, (Iterable<?>) b);
  }
  
  
  public static boolean intersect(Collection<?> a, Iterable<?> iterable) {
    for (Object ele : iterable) {
      if (a.contains(ele))
        return true;
    }
    return false;
  }

}
