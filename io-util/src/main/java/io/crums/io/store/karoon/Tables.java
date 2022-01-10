/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.io.store.karoon;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.crums.util.TaskStack;

/**
 * No frills <tt>Db</tt> implementation.
 */
public class Tables<T extends TableStore> implements Db<T> {
  
  private final Map<String, T> tableMap;

  /**
   * Constructs an instance with the given tables.
   * 
   * @param tables collection or array of tables
   * 
   * @throws IllegalArgumentException if a table with a given name is twice present in the argument
   */
  public Tables(Iterable<T> tables) {
    HashMap<String, T> map = new HashMap<>();
    for (T table : tables) {
      if (map.put(table.name(), table) != null)
        throw new IllegalArgumentException("duplicate table name: " + table.name());
    }
    this.tableMap = Collections.unmodifiableMap(map);
  }
  
  
  
  
  public Map<String, T> getTables() {
    
    return tableMap;
  }


  /**
   * {@inheritDoc}
   * 
   * <h3>Implementation Note</h3>
   */
  @Override
  public void close() throws IOException {
    
    TaskStack closer = new TaskStack();
    closer.pushClose(tableMap.values());
    closer.close();
  }


  @Override
  public boolean isOpen() {
    
    for (T table : tableMap.values())
      if (!table.isOpen())
        return false;
    
    return true;
  }

}
