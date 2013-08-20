/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.test;

import java.lang.reflect.Method;

/**
 * 
 * @author Babak
 */
public class TestHelper {
  
  private TestHelper() { }
  
  
  public static String method(Object instanceOfPrivateMethodType) {
    Method method = instanceOfPrivateMethodType.getClass().getEnclosingMethod();
    if (method == null)
      throw new IllegalArgumentException("not an instance of a private method type: " + instanceOfPrivateMethodType);
    return method.getName();
  }

}
