/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.util;

/**
 * Convenience base class for unchecked exceptions generated
 * using factory methods.
 * 
 * @see #fillInStackTrace()
 */
@SuppressWarnings("serial")
public abstract class FactoryException extends RuntimeException {

  public FactoryException() {
  }

  public FactoryException(String message) {
    super(message);
  }

  public FactoryException(Throwable cause) {
    super(cause);
  }

  public FactoryException(String message, Throwable cause) {
    super(message, cause);
  }

  public FactoryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }


  /**
   * Overridden so you can generate it by calling some factory method
   * and then throwing it after calling this method in one
   * swoop. E.g.
   * 
   * <pre>
   *    throw makeException(..).fillInStackTrace();
   * </pre>
   * 
   * <h4>Superclass documentation</h4>
   * {@inheritDoc}
   */
  @Override
  public synchronized RuntimeException fillInStackTrace() {
    super.fillInStackTrace();
    return this;
  }
  
  
  

}
