/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.util;

import java.util.AbstractList;
import java.util.Collections;
import java.util.List;

/**
 * 
 * @author Babak
 */
public class CollectionUtils {

  private CollectionUtils() {   }
  
  
  public static <T> List<T> asReadOnlyList(final T[] array) {
    if (array.length == 0)
      return Collections.emptyList();
    
    return new AbstractList<T>() {
      @Override
      public T get(int index) {
        return array[index];
      }
      @Override
      public int size() {
        return array.length;
      }
    };
  }

}
