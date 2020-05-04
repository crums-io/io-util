/*
 * Copyright 2020 Babak Farhang
 */
package com.gnahraf.io.store.karoon;


import java.io.IOException;
import java.nio.channels.Channel;
import java.util.Map;

/**
 * A group of related tables ({@linkplain TableStore}s). For a quick wrapper implementation, see
 * {@linkplain Tables}.
 */
public interface Db<T extends TableStore> extends Channel {

  /**
   * Returns an immutable mapping from table names to tables.
   */
  Map<String, T> getTables();
  

  /**
   * {@inheritDoc}
   * 
   * <h3>Implementation Semantics</h3>
   * 
   * <p>
   * Closes all the {@linkplain #getTables() tables}. The order in which tables are closed is undefiined.
   * </p>
   * 
   * @throws IOException
   */
  @Override
  void close() throws IOException;
  
  
  /**
   * {@inheritDoc}
   * 
   * <h3>Implementation Semantics</h3>
   * 
   * <p>
   * Determines whether the {@linkplain #getTables() tables} are open.
   * </p>
   * 
   * @return <tt>true</tt> iff <em>all</em> the {@linkplain #getTables() tables} are open
   */
  @Override
  boolean isOpen();

}
