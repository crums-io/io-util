/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.util;

import java.util.Iterator;


/**
 * An iterator over a closeable resource.
 */
public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {

  /**
   * Closes the resource. If it must, throws a {@code RuntimeException}.
   */
  @Override
  void close();

}
