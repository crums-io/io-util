/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util;

/**
 * If a type performs an inherently dangerous operation, you might want to
 * guard against accidental (or malicious) subtypes by extending this base class.
 */
public class UnanonymousType {

  /**
   * 
   */
  protected UnanonymousType() {
    Class<?> derivedType = getClass();
    if (derivedType.isAnonymousClass())
      throw new RuntimeException(
          derivedType.getSuperclass().getName() + " forbids anonymous subclasses");
    
  }

}
