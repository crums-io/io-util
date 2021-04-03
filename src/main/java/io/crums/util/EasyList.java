/*
 * Copyright 2019 Babak Farhang
 */
package io.crums.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Adds a few convenience methods to {@linkplain ArrayList}. It would be better
 * if these were defined as default interface methods, but I don't get to redefine
 * {@linkplain java.util.List}.
 */
@SuppressWarnings("serial")
public class EasyList<T> extends ArrayList<T> {

  public EasyList() {
  }

  public EasyList(int initialCapacity) {
    super(initialCapacity);
  }

  public EasyList(Collection<T> c) {
    super(c);
  }

  public EasyList(T object) {
    add(object);
  }
  
  
  /**
   * Returns the first element.
   * 
   * @return {@code get(0)}
   * @throws IndexOutOfBoundsException if empty
   */
  public T first() throws IndexOutOfBoundsException {
    return get(0);
  }
  
  /**
   * Returns the last element.
   * 
   * @return {@code get(size() - 1)}
   * @throws IndexOutOfBoundsException if empty
   */
  public T last() throws IndexOutOfBoundsException {
    return get(size() - 1);
  }
  
  
  /**
   * Removes and returns the last element.
   * 
   * @return {@code remove(size() - 1)}
   * @throws IndexOutOfBoundsException if empty
   */
  public T removeLast() throws IndexOutOfBoundsException {
    return remove(size() - 1);
  }
  
  /**
   * @param toIndex the ending index (exc): &ge; 0 and &le; {@code size()}
   * 
   * @return {@code subList(0, toIndex)}
   */
  public List<T> headList(int toIndex) throws IndexOutOfBoundsException {
    return subList(0, toIndex);
  }
  
  /**
   * @param fromIndex the starting index (inc): &ge; 0 and &le; {@code size()}
   * 
   * @return {@code subList(fromIndex, size())}
   */
  public List<T> tailList(int fromIndex) throws IndexOutOfBoundsException {
    return subList(fromIndex, size());
  }

}
