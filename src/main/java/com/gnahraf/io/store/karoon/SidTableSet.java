/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.gnahraf.io.store.table.SortedTable;
import com.gnahraf.io.store.table.SortedTable.Searcher;
import com.gnahraf.io.store.table.TableSetD;
import com.gnahraf.io.store.table.del.DeleteCodec;
import com.gnahraf.io.store.table.order.RowOrder;
import com.gnahraf.util.CollectionUtils;

/**
 * 
 * @author Babak
 */
public class SidTableSet extends TableSetD {
  
  private final List<SidTable> sidTables;
  private final long commitId;

  /**
   * Constructs an empty instance.
   */
  public SidTableSet(RowOrder order, int rowWidth, DeleteCodec deleteCodec, long commitId) {
    super(order, rowWidth, deleteCodec);
    sidTables = Collections.emptyList();
    this.commitId = commitId;
  }


  @Override
  protected Searcher getSearcher(SortedTable table) throws IOException {
    return ((SidTable) table).getSearcher();
  }


  /**
   * Constructs an instance with a singleton table.
   */
  public SidTableSet(SidTable table, DeleteCodec deleteCodec, long commitId) throws IOException {
    super(table, deleteCodec);
    sidTables = Collections.singletonList(table);
    this.commitId = commitId;
  }


  /**
   * Constructs an instance with the given <tt>tables</tt>. The order of the tables is
   * significant: higher index tables override equal rows found in the lower index ones.
   */
  public SidTableSet(SidTable[] tables, DeleteCodec deleteCodec, long commitId) throws IOException {
    super(tables, deleteCodec);
    sidTables = CollectionUtils.asReadOnlyList(tables.clone());
    this.commitId = commitId;
  }
  
  public final List<SidTable> sidTables() {
    return sidTables;
  }
  
  /**
   * Returns {@linkplain #sidTables()}<tt>.toString()</tt>.
   */
  @Override
  public String toString() {
    return sidTables.toString();
  }


  public final long getCommitId() {
    return commitId;
  }
  
  
  public final List<Long> getTableIds() {
    ArrayList<Long> ids = new ArrayList<>(sidTables.size());
    for (int i = 0; i < sidTables.size(); ++i)
      ids.add(sidTables.get(i).id());
    return ids;
  }

}
