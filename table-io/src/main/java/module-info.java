module io.crums.io.store {
  
  requires transitive io.crums.util.xp;
  
  exports io.crums.io.block;
  exports io.crums.io.store;
  exports io.crums.io.store.ks;
  exports io.crums.io.store.table;
  exports io.crums.io.store.table.del;
  exports io.crums.io.store.table.iter;
  exports io.crums.io.store.table.merge;
  exports io.crums.io.store.table.order;
  
}