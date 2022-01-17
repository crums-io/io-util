/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.io.store.karoon.multi;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.crums.io.store.karoon.Db;
import io.crums.io.store.karoon.TableStore;
import io.crums.util.TaskStack;

/**
 * A union of chained <tt>Db</tt>s.
 * In this model, only the frontier db is written to. The others are only used for reads.
 * The named {@linkplain #getTables() tables} available in this view are those in frontier db.
 * These tables in turn are chained views on the same table schema in the chained dbs.
 * 
 * @see TableStoreChain
 */
public class DbChain implements Db<TableStore> {
  
  private final Db<? extends TableStore>[] chain;
  private final Map<String, TableStore> tableMap;

  /**
   * Creates a new instance with the given collection of backing instances.
   * That last db [in the iteration] of the collection is the frontier instance: writes
   * go in there.
   */
  @SuppressWarnings("unchecked")
  public DbChain(Collection<Db<? extends TableStore>> dbs) throws IllegalArgumentException {
    
    final int length = dbs.size();
    if (length < 1)
      throw new IllegalArgumentException("empty db collection");
    
    // populate the chain
    //
    this.chain = (Db<TableStore>[]) new Db[length];
    {
      int index = 0;
      for (Db<?> db : dbs)
        chain[index++] = db;
    }
    
    //
    // build the table chains that go into the table map..
    //
    
    HashMap<String, TableStore> map = new HashMap<>();
    
    // the frontier db determines which tables are to be included..
    //
    for (String tableName : chain[length - 1].getTables().keySet()) {
      
      ArrayList<TableStore> tables = new ArrayList<>(length);
      
      for (Db<?> db : chain) {
        TableStore t = db.getTables().get(tableName);
        if (t == null)
          throw new IllegalArgumentException("missing '" + tableName + "' table in chained db " + db);
        tables.add(t);
      }
      
      map.put(tableName, new TableStoreChain(tables)); // <-- argument validation is in constructor
    }
    
    
    tableMap = Collections.unmodifiableMap(map);
  }

  @Override
  public Map<String, TableStore> getTables() {
    return tableMap;
  }

  @SuppressWarnings("resource") // blind compiler can't find close() under its nose :/
  @Override
  public void close() throws IOException {
    new TaskStack().pushClose(chain).close();
  }

  /**
   * Checks to see if the <tt>Db</tt>s internal to the chain are open.
   * 
   * @return <tt>true</tt> iff every db in the chain is open.
   */
  @Override
  public boolean isOpen() {
    int index = chain.length;
    while (index-- > 0 && chain[index].isOpen());
    return index == -1;
  }

}
