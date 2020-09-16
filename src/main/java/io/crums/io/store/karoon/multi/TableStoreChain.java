/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.io.store.karoon.multi;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.util.Collection;
import java.util.Objects;
import java.util.logging.Logger;

import io.crums.io.buffer.Covenant;
import io.crums.io.store.karoon.Db;
import io.crums.io.store.karoon.TableStore;
import io.crums.io.store.table.TableSet;
import io.crums.io.store.table.del.DeleteCodec;
import io.crums.io.store.table.iter.Direction;
import io.crums.io.store.table.order.RowOrder;

/**
 * A chain of tables presented as one. This is a familiar pattern (see {@linkplain TableSet}, for example).
 * In this model, one table (called frontier in code) is the one written to. The others are only used
 * for reads.
 * 
 * <h2>Reads</h2>
 * <p>
 * Row lookups by key ({@linkplain #getRow(ByteBuffer)}) are performed front to back: the first table to return a hit
 * wins. Ordered iteration ({@linkplain #nextRow(ByteBuffer, Direction, boolean)}) scans all tables before returning
 * the next row: if 2 tables return the same row (i.e. with the same key; other "columns" maybe different), the one
 * nearest the end of the chain wins.
 * </p><p>
 * </p>
 * 
 * <h2>Writes</h2>
 * <p>
 * The frontier table is at the end of the chain and is the only one changing state. All writes go there.
 * Delete semantics are broken: you can't delete what's in the backing (read-only) tables at the beginning of
 * the chain. It's easy enough to fix, but not a pressing concern at this time.
 * </p>
 */
public class TableStoreChain implements TableStore {
  
  private final Logger LOG = Logger.getLogger(TableStoreChain.class.getName());
  
  private final TableStore[] chain;

  /**
   * Creates a new instance with the given collection of backing instances.
   * That last table [in the iteration] of the collection is the frontier instance: writes
   * go in there.
   */
  public TableStoreChain(Collection<? extends TableStore> tables) {
    chain = new TableStore[tables.size()];
    int index = 0;
    for (TableStore table : tables) 
      chain[index++] = table;
    
    checkChain();
  }
  
  
  // sanity check arguments
  private void checkChain() throws IllegalArgumentException {
    int index = chain.length - 1;
    if (index < 0)
      throw new IllegalArgumentException("empty collection of tables");
    TableStore frontier = chain[index];
    while (index-- > 0) {
      TableStore t = chain[index];
      if (!frontier.name().equals(t.name()))
        throw new IllegalArgumentException("names clash: " + frontier.name() + " != " + t.name());
      if (!frontier.rowOrder().equals(t.rowOrder()))
        throw new IllegalArgumentException("row orders  clash: " + frontier.rowOrder() + " != " + t.rowOrder());
      if (frontier.rowWidth() != t.rowWidth())
        throw new IllegalArgumentException("row widths  clash: " + frontier.rowOrder() + " != " + t.rowOrder());
      if (!Objects.equals( frontier.deleteCodec(), t.deleteCodec()))
        throw new IllegalArgumentException("delete codecs  clash: " + frontier.deleteCodec() + " != " + t.deleteCodec());
    }
  }


  /**
   * Checks to see if the tables internal to the chain are open.
   * 
   * @return <tt>true</tt> iff every table in the chain is open.
   */
  @Override
  public boolean isOpen() {
    int index = chain.length;
    while (index-- > 0 && chain[index].isOpen());
    return index == -1;
  }


  /**
   * Noop. But nags with a warning log message.
   * <p>
   * <em>TODO: Reconsider merit of extending {@linkplain Channel} in {@linkplain TableStore} interface.
   * Seems it doesn't belong here: the unit to close would be the group of tables {@linkplain Db}.
   * </p>
   * 
   * @see #isOpen()
   */
  @Override
  public void close() throws IOException {
    LOG.warning("close operation deliberately ignored");
//    for (int index = chain.length; index-- > 0;)
//      chain[index].close();
  }


  @Override
  public String name() {
    return chain[0].name();
  }


  @Override
  public int rowWidth() {
    return chain[0].rowWidth();
  }


  @Override
  public RowOrder rowOrder() {
    return chain[0].rowOrder();
  }


  @Override
  public DeleteCodec deleteCodec() {
    return chain[0].deleteCodec();
  }


  @Override
  public ByteBuffer getRow(ByteBuffer key) throws IOException {
    for (int index = chain.length; index-- > 0; ) {
      ByteBuffer row = chain[index].getRow(key);
      if (row != null)
        return row;
    }
    return null;
  }


  @Override
  public ByteBuffer nextRow(ByteBuffer key, Direction direction, boolean includeKey) throws IOException {
    
    ByteBuffer hit = null;
    
    for (int index = chain.length; index-- > 0; ) {
      
      ByteBuffer row = chain[index].nextRow(key, direction, includeKey);
      if (row != null && (hit == null || direction.effectiveComp(rowOrder().compare(row, hit)) < 0))
        hit = row;
    }
    
    return hit;
  }


  @Override
  public void setRow(ByteBuffer row, Covenant promise) throws IOException {
    frontier().setRow(row, promise);
  }


  @Override
  public void setRows(ByteBuffer rows, Covenant promise) throws IOException {
    frontier().setRows(rows, promise);
  }


  @Override
  public void deleteRow(ByteBuffer key) throws IOException {
    frontier().deleteRow(key);
  }
  
  
  private TableStore frontier() {
    return chain[chain.length - 1];
  }

}
