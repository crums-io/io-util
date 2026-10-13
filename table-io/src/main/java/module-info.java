module io.crums.util.io.store {
  
  requires transitive io.crums.util.xp;
  requires transitive io.crums.util.io.buffer;
  requires transitive io.crums.util.io;

  exports io.crums.util.io.block;
  exports io.crums.util.io.store;
  exports io.crums.util.io.store.ks;
  exports io.crums.util.io.store.table;
  exports io.crums.util.io.store.table.del;
  exports io.crums.util.io.store.table.iter;
  exports io.crums.util.io.store.table.merge;
  exports io.crums.util.io.store.table.order;
  
}