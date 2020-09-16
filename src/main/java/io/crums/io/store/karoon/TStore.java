/*
 * Copyright 2013-2020 Babak Farhang 
 */
package io.crums.io.store.karoon;


import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import io.crums.io.FileUtils;
import io.crums.io.IoStateException;
import io.crums.io.buffer.Covenant;
import io.crums.io.store.karoon.merge.MergePolicy;
import io.crums.io.store.karoon.merge.TableLifecycleListener;
import io.crums.io.store.karoon.merge.TableMergeEngine;
import io.crums.io.store.karoon.merge.TableRegistry;
import io.crums.io.store.ks.CachingKeystone;
import io.crums.io.store.ks.Keystone;
import io.crums.io.store.table.SortedTable;
import io.crums.io.store.table.TableSet;
import io.crums.io.store.table.del.DeleteCodec;
import io.crums.io.store.table.iter.Direction;
import io.crums.io.store.table.iter.RowIterator;
import io.crums.io.store.table.iter.TableSetIterator;
import io.crums.io.store.table.order.RowOrder;
import io.crums.math.stats.MovingAverage;
import io.crums.util.TaskStack;
import io.crums.util.cc.throt.FuzzySpeed;
import io.crums.util.cc.throt.FuzzyThrottler;

/**
 * Single thread (but fail safe) access to a logical, sorted table on disk.
 * This is implemented as a stack of sorted tables. The tables are write-once.
 * Behind the scenes, there are typically 2 background threads merging tables
 * into bigger ones: one for merging young tables, another for merging more
 * mature ones. The design is inspired by how Apache Lucene merges tables--except
 * here the data model is a good deal simpler so we are able to pull off a live
 * view.
 * <h4>Write-ahead Log</h4>
 * <p>
 * A <em>frontier</em> unsorted table serves as a write-ahead log. This is kept small
 * enough so that it can fit (in sorted order) in memory.
 * </p>
 * <h4>Automatic Throttling</h4>
 * <p>
 * Early tests on the write path indicated as you'd throw more and more data faster and
 * faster at a <tt>TStore</tt> instance, beyond a certain speed, performance would
 * start to degrade as the background merging threads would fail to keep up. This wasn't
 * a surprise: every software system must somehow throttle under maximum load, either
 * "naturally" or by design. Here it had to be designed in.
 * </p><p>
 * Each instance uses a fuzzy controller to set the throttle--the maximum throughput
 * (insertion) rate. It watches how many backing table files an instance has at any moment, how
 * fast that number is rising and depending on how hot the situation has become slams, or
 * touches lightly, on the brakes. Under load, the goal of the controller is to keep
 * the backing-tables-count near {@linkplain  MergePolicy#getEngineOverheatTableCount()}.
 * </p><p>
 * There's no throttling on the reads: the read path is "naturally" throttled.
 * </p>
 * <h4>TODO</h4>
 * <p>
 * <ul>
 * <li>
 * <p>
 * Fix threading model: make it message based -- collapse messages by recognizing
 * duplicates; ignore messages for which some action is already being taken. Goal is
 * fewer waiting threads: 2 threads per instance is a bit heavy handed if you have many
 * <tt>TStore</tt>instances.
 * </p>
 * </li>
 * <li>
 * More documentation. The author forgets.
 * </li>
 * </ul>
 * </p>
 */
public class TStore implements TableStore {
  
  public class TmeContext {
    
    private TmeContext() { }
    
    public long newTableId() throws IOException {
      return tableCounter.increment(1);
    }
    
    public File newTablePath(long tableId) throws IOException {
      return getSortedTablePath(tableId, false);
    }
    
    public void tablesMerged(List<Long> srcIds, SidTable result) throws IOException {
      processMerged(srcIds, result);
    }
    
    public void discardTable(long tableId) {
      File tableFile = getSortedTablePath(tableId);
      discardFile(tableFile);
    }
    
    public TStore store() {
      return TStore.this;
    }
    
    public TableRegistry tableRegistry() {
      return tableRegistry;
    }
  }
  

  private class LoadMeter extends FuzzySpeed {
    
    private final MovingAverage tableCount;;
    private CommitRecord commit;
    
    LoadMeter() {
      commit = currentCommit;
      tableCount = new MovingAverage(10, commit.getTableIds().size());
    }
    
    
    public synchronized void update() {
      if (commit != currentCommit) {
        commit = currentCommit;
        tableCount.observe(commit.getTableIds().size());
      }
    }

    @Override
    public synchronized double accelerating() {
      long delta = delta();
      if (delta <= 0)
        return 0;
      return absAcc(delta);
    }
    
    private long delta() {
      return tableCount.lastValue() - tableCount.valueAt(0);
    }
    
    private double absAcc(long delta) {
      if (delta < 2)
        return 0.1;
      else if (delta < 3)
        return 0.3;
      else if (delta < 5)
        return 0.6;
      else if (delta < 8)
        return 0.8;
      else
        return 1;
    }

    @Override
    public synchronized double cruising() {
      long delta = Math.abs(delta());
      if (delta < 2)
        return 1;
      else if (delta < 3)
        return 0.9;
      else if (delta < 4)
        return 0.75;
      else if (delta < 5)
        return 0.5;
      else if (delta < 7)
        return 0.25;
      else
        return 0;
    }

    @Override
    public synchronized double decelerating() {
      long delta = delta();
      if (delta >= 0)
        return 0;
      return absAcc(-delta);
    }

    @Override
    public synchronized double tooFast() {
      long overheatCount = overheatCount();
          
      if (overheatCount <= -2)
        return 0;
      
      if (overheatCount < 0)
        return 0.1;
      else if (overheatCount < 1)
        return 0.2;
      else if (overheatCount < 3)
        return 0.4;
      else if (overheatCount < 5)
        return 0.5;
      else if (overheatCount < 7)
        return 0.75;
      else
        return 1;
    }
    
    
    private long overheatCount() {
      return tableCount.lastValue() - config.getMergePolicy().getEngineOverheatTableCount();
    }
    
    private long avgOverheatCount() {
      return tableCount.average() - config.getMergePolicy().getEngineOverheatTableCount();
    }

    @Override
    public synchronized double justRight() {
      long avgOverheatCount = avgOverheatCount();
      long overheatCount = overheatCount();
      if (overheatCount <= 0) {
        if (avgOverheatCount <= 3)
          return 1;
        else if (avgOverheatCount < 5)
          return 0.9;
        else if (avgOverheatCount < 7)
          return 0.75;
        else
          return 0.5;
      } else if (overheatCount < 3)
        return 0.8;
      else if (overheatCount < 7)
        return 0.4;
      else
        return 0;
    }

    @Override
    public synchronized double tooSlow() {
      long lastTableCount = tableCount.lastValue();
      long overheatCount = config.getMergePolicy().getEngineOverheatTableCount();
      if (lastTableCount >= overheatCount / 2)
        return 0;
      else if (lastTableCount >= overheatCount / 3)
        return 0.3;
      else if (lastTableCount >= overheatCount / 4)
        return 0.6;
      else
        return 1;
    }
    
  }
  
  protected final static Logger LOG = Logger.getLogger(TStore.class.getName());
  private final static long INIT_COUNTER_VALUE = 0L;
  
  public final static String COUNTERS_FILENAME = "tc.counts";
  public final static String COMMIT_EXT = "cmmt";
  public final static String COMMIT_PREFIX = "C";
  

  public final static String TABLE_PREFIX = "T";
  public final static String SORTED_TABLE_EXT = "stbl";
  public final static String UNSORTED_TABLE_EXT = "utbl";
  
  
  
  
  
  
  // I N S T A N C E   M E M B E R S
  
  
  private final TaskStack closer = new TaskStack(LOG);
  private final Object apiLock = new Object();
  private final Object backSetLock = new Object();
  
  private final TStoreConfig config;
  private final Keystone tableCounter;
  private final Keystone commitNumber;
  private final Keystone walTableNumber;
  
  private final TableMergeEngine tableMergeEngine;
  private final LoadMeter loadMeter;
  private final FuzzyThrottler throttle;
  private final TableRegistry tableRegistry;
  
  
  private WriteAheadTableBuilder writeAhead;
  private SidTableSet activeTableSet;
  private volatile CommitRecord currentCommit;
  
  private final Object commitWatch = new Object();
  
  
  public TStore(TStoreConfig config, boolean create) throws IOException {
    if (config == null)
      throw new IllegalArgumentException("null config");
    
    if (config.isReadOnly() && create)
      throw new IllegalArgumentException("attempt to create a read read-only instance");
    
    this.config = config;
    boolean failed = true;
    try {
      if (create)
        FileUtils.ensureDir(config.getRootDir());
      else
        FileUtils.assertDir(config.getRootDir());
      
      File counterFile = new File(config.getRootDir(), COUNTERS_FILENAME);
      if (counterFile.exists()) {
        FileUtils.assertFile(counterFile);
        @SuppressWarnings("resource")
        FileChannel file = new RandomAccessFile(counterFile, "rw").getChannel();
        file.position(0);
        tableCounter = new CachingKeystone(Keystone.loadInstance(file));
        closer.pushClose(tableCounter);
        commitNumber = new CachingKeystone(Keystone.loadInstance(file));
        walTableNumber = new CachingKeystone(Keystone.loadInstance(file));
        
      } else if (create) {
        @SuppressWarnings("resource")
        FileChannel file = new RandomAccessFile(counterFile, "rw").getChannel();
        file.position(0);
        tableCounter = new CachingKeystone(Keystone.createInstance(file, INIT_COUNTER_VALUE));
        closer.pushClose(tableCounter);
        commitNumber = new CachingKeystone(Keystone.createInstance(file, INIT_COUNTER_VALUE));
        walTableNumber = new CachingKeystone(Keystone.createInstance(file, INIT_COUNTER_VALUE));
      } else {
        throw new FileNotFoundException("expected file does not exist: " + counterFile.getAbsolutePath());
      }
      


      this.currentCommit = loadCommitRecord(commitNumber.get());
      this.activeTableSet = load(currentCommit);
      this.loadMeter = new LoadMeter();
      this.throttle = new FuzzyThrottler(loadMeter);
      
      {
        TableLifecycleListener lifecycleListener = new TableLifecycleListener() {
          @Override
          public void released(long tableId) {
            File tableFile = getSortedTablePath(tableId);
            discardFile(tableFile);
          }
          @Override
          public void inited(long tableId) {
          }
        };
        
        this.tableRegistry = new TableRegistry(lifecycleListener);
      }
      
      // find the last working (unsorted) table, if any..
      long walTableId = walTableNumber.get();
      File writeAheadFile = getWriteAheadPath(walTableId);
      if (writeAheadFile.exists()) {
        
        // TODO: there might be a race with the merger thread
        //       i.e. we haven't yet ensured the merger thread could not have merged walTableId
        //       away before the last instance was abnormally shutdown
        if (currentCommit.getTableIds().contains(walTableId)) {
          LOG.warning("Recovering from abnormal shutdown..");
          discardFile(writeAheadFile);
        } else {
          writeAhead = new WriteAheadTableBuilder(
              config.getRowWidth(), config.getRowOrder(), writeAheadFile);
          // 
          File sortedTableFile = getSortedTablePath(walTableId);
          FileUtils.delete(sortedTableFile);
        }
      }
      
      if (writeAhead == null) {
        setNextWriteAhead();
      }
      
      this.tableMergeEngine = new TableMergeEngine(new TmeContext(), config.getMergeThreadPool());
      closer.pushClose(tableMergeEngine);
      tableMergeEngine.start();
      
      failed = false;
      
    } finally {
      if (failed) {
        close();
        LOG.severe("Init failed [config=" + config + ", create=" + create + "]");
      }
    }
  }
  
  
  private void setNextWriteAhead() throws IOException {
    long walTableId = tableCounter.increment(1);
    File writeAheadFile = getWriteAheadPath(walTableId);
    FileUtils.assertDoesntExist(writeAheadFile);
    writeAhead = new WriteAheadTableBuilder(
        config.getRowWidth(), config.getRowOrder(), writeAheadFile);
    walTableNumber.set(walTableId);
  }

  
  @Override
  public String name() {
    return config.getRootDir().getName();
  }
  
  
  @Override
  public int rowWidth() {
    return config.getRowWidth();
  }
  
  
  @Override
  public RowOrder rowOrder() {
    return config.getRowOrder();
  }
  
  
  @Override
  public DeleteCodec deleteCodec() {
    return config.getDeleteCodec();
  }
  
  
  @Override
  public ByteBuffer getRow(ByteBuffer key) throws IOException {
    ByteBuffer row;
    synchronized (apiLock) {
      row = writeAhead.getRow(key);
      
      if (row == null) {
        synchronized (backSetLock) {
          row = activeTableSet().getRow(key);
        }
      }
    }
    
    if (row != null && hasDc() && config.getDeleteCodec().isDeleted(row))
      return null;
    else
      return row;
  }
  
  
  private boolean hasDc() {
    return config.getDeleteCodec() != null;
  }
  
  
  
  @Override
  public ByteBuffer nextRow(ByteBuffer key, Direction direction, boolean includeKey) throws IOException {
    
    ByteBuffer war; // row from write-ahead table
    ByteBuffer sr;  // row from sorted-table set
    
    synchronized (apiLock) {
      
      RowIterator walIterator = writeAhead.iterator(key, direction, includeKey);
      war = walIterator.next();
      
      synchronized (backSetLock) {
        
        TableSetIterator iter = activeTableSet().iterator();
        iter.init(key, direction);
        
        sr = iter.next();
        if (sr != null && !includeKey && config.getRowOrder().compare(key, sr) == 0)
          sr = iter.next();
        
        
        if (hasDc()) {
          while (war != null && config.getDeleteCodec().isDeleted(war)) {
            if (sr != null && config.getRowOrder().compare(war, sr) == 0)
              sr = iter.next();
            war = walIterator.next();
          }
        }
      }
      
    }
    
    if (war == null)
      return sr;
    else if (sr == null)
      return war;
    else
      return direction.effectiveComp(config.getRowOrder().compare(war, sr)) > 0 ? sr : war;
  }

  
  @Override
  public void deleteRow(ByteBuffer key) throws IOException {
    deleteRow(key, true);
  }
  
  
  public void deleteRow(ByteBuffer key, boolean checkExists) throws IOException {
    
    if (!hasDc())
      throw new UnsupportedOperationException("append/overwrite-only table");

    synchronized (apiLock) {
      
      if (checkExists) {
        
        ByteBuffer row = writeAhead.getRow(key);
        
        // if the row is already tombstoned, return right away
        if (row != null && config.getDeleteCodec().isDeleted(row))
          return;
        
        ByteBuffer backRow = activeTableSet().getRow(key);
        if (backRow == null || config.getDeleteCodec().isDeleted(backRow)) {
          
          if (row != null) {
            // the wal contains this key, but the backset doesn't..
            // remove the in-memory row, but write a tombstone to the wal
            config.getDeleteCodec().markDeleted(key);
            writeAhead.writeAheadButRemove(key);
            
          }
          
          return;
        }
      } // if (checkExists) {
      
      config.getDeleteCodec().markDeleted(key);
      setRow(key);
    }
  }
  

//  /**
//   * Inserts or updates the given <tt>row</tt> with no promise/covenant. Shorthand for
//   * {@linkplain #setRow(ByteBuffer, Covenant) setRow(row, Covenant.NONE)}
//   * 
//   * @param row
//   *        the remaining bytes in this buffer represent the row being input. The
//   *        remaining bytes must be exactly equal to
//   *        {@linkplain TStoreConfig#getRowWidth() row width}.
//   */
//  @Override
//  public void setRow(ByteBuffer row) throws IOException {
//    setRow(row, Covenant.NONE);
//  }
  

  
  @Override
  public void setRow(ByteBuffer row, Covenant promise) throws IOException {
    synchronized (apiLock) {
      writeAhead.putRow(row, promise);
      manageWriteAhead();
    }
  }
  
  @Override
  public void setRows(ByteBuffer rows, Covenant promise) throws IOException {
    synchronized (apiLock) {
      writeAhead.putRows(rows, promise);
      manageWriteAhead();
    }
  }
  

  
  
  /**
   * The <tt>apiLock</tt> is already held.
   */
  private void manageWriteAhead() throws IOException {
    if (writeAhead.getWalSize() < config.getMergePolicy().getWriteAheadFlushTrigger())
      return;
    
    synchronized (backSetLock) {
      long walId = deriveFileId(
          TABLE_PREFIX, UNSORTED_TABLE_EXT, writeAhead.getWriteAheadFile());
      if (walId != walTableNumber.get())
        throw new IoStateException(
            "assertion failure. expected walId " + walId + "; actual was " + walTableNumber.get());
      File sortedWalFile = getSortedTablePath(walId, false);
      FileChannel ch = new FileOutputStream(sortedWalFile).getChannel();
      try {
        writeAhead.flush(ch);
      } finally {
        ch.close();
      }
      writeAhead.close();
      List<SidTable> tables = activeTableSet().sidTables();
      SidTable[] newActiveTables = new SidTable[tables.size() + 1];
      List<Long> tableIds = new ArrayList<>(tables.size() + 1);
      for (int i = 0; i < tables.size(); ++i) {
        SidTable table = tables.get(i);
        tableIds.add(table.id());
        newActiveTables[i] = table;
      }
      tableIds.add(walId);
      final long prevCommitId = commitNumber.get();
      final long commitId = prevCommitId + 1;
      File file = getCommitPath(commitId);
      CommitRecord newCommitRecord = CommitRecord.create(file, tableIds, commitId);
      // all-or-nothing commit
      commitNumber.set(commitId);
      // committed
      discardFile(writeAhead.getWriteAheadFile());
      if (prevCommitId != INIT_COUNTER_VALUE)
        discardFile(getCommitPath(prevCommitId));
      
      newActiveTables[tables.size()] = loadSortedTable(sortedWalFile, walId);
      activeTableSet(new SidTableSet(newActiveTables, config.getDeleteCodec(), commitId));
      setCurrentCommit(newCommitRecord);
      setNextWriteAhead();
    }
    this.tableMergeEngine.notifyFreshMeat();
    throttle.throttledTicker().tick();
  }
  
  
  

  /**
   * Discards a no-longer used file. Use this as a customization hook to build
   * such things as audit trails, etc. The base implementation just deletes the
   * file.
   * 
   * @throws IOException
   *         the base implementation doesn't throw this
   * @throws IllegalStateException
   *         if the <tt>file</tt> cannot be moved to the trash directory
   */
  protected void discardFile(File file) {
    if (!file.delete()) {
      LOG.warning("Failed to deleted " + file.getPath());
    }
  }
  
  private SidTableSet activeTableSet() {
    synchronized (backSetLock) {
      return activeTableSet;
    }
  }
  
  private void activeTableSet(SidTableSet tableSet) {
    synchronized (backSetLock) {
      this.activeTableSet = tableSet;
    }
  }
  
  
  private long deriveFileId(String prefix, String ext, File file) {
    String fname = file.getName();
    int idStartOffset = prefix.length();
    int idEndOffset = fname.length() - ext.length() - 1;
    return Long.parseLong(fname.substring(idStartOffset, idEndOffset));
  }
  
  
  private File getWriteAheadPath(long tableId) {
    return new File(
        config.getRootDir(),
        TABLE_PREFIX + tableId + "." + UNSORTED_TABLE_EXT);
  }



  public CommitRecord loadCommitRecord(long commitNum) throws IOException {
    if (commitNum < 0)
      throw new IllegalArgumentException("negative commit number: " + commitNum);
    if (commitNum == 0)
      return CommitRecord.INIT;

    File commitFile = getCommitPath(commitNum);
    return CommitRecord.load(commitFile, commitNum);
  }

  private File getCommitPath(long commitNumber) throws KaroonException {
    if (commitNumber < 1)
      throw new KaroonException("unexpected commit number: " + commitNumber);
    return new File(config.getRootDir(), COMMIT_PREFIX + commitNumber + "." + COMMIT_EXT);
  }
  
  
  public long currentCommitId() throws IOException {
    return commitNumber.get();
  }
  
  
  /**
   * Loads and returns the table set associated with the given commit <tt>record</tt>.
   * <em>Note the returned resource must eventually be {@linkplain TableSet#close() close}d
   * by the caller</em>: it's a fresh new instance with open streams on the file system.
   * 
   * @throws IOException
   */
  public SidTableSet load(CommitRecord record) throws IOException {
    if (record == null)
      throw new IllegalArgumentException("null commit record");
    
    if (record.getTableIds().isEmpty())
      return new SidTableSet(
          config.getRowOrder(), config.getRowWidth(), config.getDeleteCodec(), record.getId());
    
    try (TaskStack closeOnFail = new TaskStack(LOG)) {
      SidTable[] tables = new SidTable[record.getTableIds().size()];
      for (int i = 0; i < tables.length; ++i) {
        long tableId = record.getTableIds().get(i);
        File tableFile = getSortedTablePath(tableId, true);
        tables[i] = loadSortedTable(tableFile, tableId);
        closeOnFail.pushClose(tables[i]);
      }
      SidTableSet tableSet = new SidTableSet(tables, config.getDeleteCodec(), record.getId());
      closeOnFail.clear();
      return tableSet;
    }
  }
  


  private File getSortedTablePath(long tableId, boolean exists) throws IOException {
    File file = getSortedTablePath(tableId);
    if (exists)
      FileUtils.assertFile(file);
    else
      FileUtils.assertDoesntExist(file);
    return file;
  }

  private File getSortedTablePath(long tableId) {
    return new File(config.getRootDir(), getTableFilename(tableId));
  }


  /**
   * Returns the table's simple file name given its <tt>tableId</tt>. Hook for a
   * subclass wishing to change the file naming scheme.
   */
  protected String getTableFilename(long tableId) {
    return TABLE_PREFIX + tableId + "." + SORTED_TABLE_EXT;
  }
  
  
  /**
   * Returns the file size of the table with the given <tt>tableId</tt>.
   * 
   * @throws FileNotFoundException
   *         if there is no such table
   */
  public long getTableFileSize(long tableId) throws IOException {
    return getSortedTablePath(tableId, true).length();
  }
  

  /**
   * Returns the commit record of the current back set.
   */
  public CommitRecord getCurrentCommit() {
    return currentCommit;
  }


  /**
   * Loads an existing sorted table. If the table had some sort of header,
   * here's where you'd handle that. The base implementation assumes no header.
   * <em>The caller must arrange that the returned instance be eventually
   * <strong>closed</strong>; otherwise, a resource leak will occur.</em>
   * 
   * @return a read-only instance
   */
  public SidTable loadSortedTable(long tableId) throws IOException {
    File tableFile = getSortedTablePath(tableId, true);
    return loadSortedTable(tableFile, tableId);
  }
  
  
  /**
   * Loads and returns the {@linkplain SortedTable} (a <tt>SidTable</tt>) in read-only mode.
   * Invoked by {@linkplain #loadSortedTable(long)}.
   */
  protected SidTable loadSortedTable(File tableFile, long id) throws IOException {
    @SuppressWarnings("resource")
    FileChannel ch = new RandomAccessFile(tableFile, "r").getChannel();
    
    // Following test code, when uncommented, passed unit tests ~ 4/28/2020
    
    // SidTable table = new SidTable(ch, 0, config.getRowWidth(), config.getRowOrder(), id);
    // return table.sliceTable(0, table.getRowCount());
    
    return new SidTable(ch, 0, config.getRowWidth(), config.getRowOrder(), id);
  }




  public final TStoreConfig getConfig() {
    return config;
  }



  @Override
  public void close() {
    synchronized (apiLock) {
      synchronized (backSetLock) {
        if (isOpen()) {
          // FIXME: following is buggy (doesn't quite work)
          // ignoring: was a nice-to-have ..
//          commitWriteAheadOnClose();
          Closeable activeSet = activeTableSet();
          if (activeSet != null)
            closer.pushClose(activeTableSet());
          closer.close();
        }
      }
    }
    boolean finished;
    if (tableMergeEngine == null)
      finished = true;
    else
      try {
        finished = tableMergeEngine.await(3 * 1000);
      } catch (InterruptedException rx) {
        LOG.warning("interrupted on tableMergeEngine.await(3 * 1000). Ignoring..");
        finished = false;
      }
    
    if (finished)
      LOG.info(this + ": [STOPPED]");
  }


  @Override
  public boolean isOpen() {
    return closer.isOpen();
  }
  
  
  
  protected void processMerged(List<Long> srcIds, SidTable result) throws IOException {
    // check args..
    // failure here represents a bug, but we still want to fail fast
    if (srcIds == null || srcIds.size() < 2)
      throw new IllegalArgumentException("srcIds: " + srcIds);
    if (result == null)
      throw new IllegalArgumentException("null result");
    if (!result.isOpen())
      throw new IllegalArgumentException("result not open");
    
    // it's theoretically possible that 2 or more tables cancel each other out perfectly
    // Rather than handle this case, we'll just wait until until the condition changes..
    if (result.getRowCount() == 0) {
      LOG.warning("Discarding empty merge result. This should be a rare corner case. srcIds=" + srcIds);
      result.close();
      return;
    }
    
    TaskStack closer = new TaskStack();
    boolean failed = true;
    CommitRecord preMergeCommit;
    try {
      synchronized (backSetLock) {
        if (!isOpen())
          return;
        
        preMergeCommit = getCurrentCommit();
        final List<Long> tableIds = preMergeCommit.getTableIds();
        if (tableIds.contains(result.id()))
          throw new IllegalArgumentException("result=" + result + ", currentCommit=" + preMergeCommit);
        
        final int insertionOff = tableIds.indexOf(srcIds.get(0));
        
        // sanity check
        if (insertionOff == -1 ||
            insertionOff + srcIds.size() > tableIds.size() ||
            !tableIds.subList(insertionOff, srcIds.size() + insertionOff).equals(srcIds))
          throw new IllegalArgumentException("srcIds=" + srcIds + "; currentCommit=" + preMergeCommit);

        SidTable[] postMergeStack = new SidTable[tableIds.size() - srcIds.size() + 1];
        final List<SidTable> preMergeStack = activeTableSet().sidTables();
        
        int index = 0;
        
        while (index < insertionOff) {
          postMergeStack[index] = preMergeStack.get(index);
          ++index;
        }
        postMergeStack[index] = result;
        while (index < insertionOff + srcIds.size())
          closer.pushClose(preMergeStack.get(index++));
        for (int j = insertionOff; ++j < postMergeStack.length; )
          postMergeStack[j] = preMergeStack.get(index++);
        
        final long postCommitId = commitNumber.get() + 1;
        
        SidTableSet postMergeTableSet =
            new SidTableSet(postMergeStack, config.getDeleteCodec(), postCommitId);
        
        File commitFile = getCommitPath(postCommitId);
        
        CommitRecord postCommitRecord =
            CommitRecord.create(commitFile, postMergeTableSet.getTableIds(), postCommitId);
        
        // commit
        commitNumber.set(postCommitId);
        activeTableSet(postMergeTableSet);
        setCurrentCommit(postCommitRecord);
      } // synchronized (backSetLock) { .. }
      
      failed = false;
    } finally {
      if (failed)
        closer.clear().pushClose(result);
      closer.close();
    }
    
    // if we get this far we haven't failed
    // clean up the left overs..
    if (preMergeCommit.getFile() != null)
      discardFile(preMergeCommit.getFile());
//    for (long srcId : srcIds)
//      discardFile(getSortedTablePath(srcId));
  }
  
  

  
  private void notifyNewCommit() {
    synchronized (commitWatch) {
      commitWatch.notifyAll();
    }
  }
  
  private void setCurrentCommit(CommitRecord newCommitRecord) {
    currentCommit = newCommitRecord;
    loadMeter.update();
    throttle.updateThrottle();
    notifyNewCommit();
  }
  
  public void waitForCommitChange(long commitId) throws InterruptedException {
    waitForCommitChange(commitId, Long.MAX_VALUE);
  }
  
  public void waitForCommitChange(long commitId, long timeOutMillis) throws InterruptedException {
    if (currentCommit.getId() != commitId)
      return;
    synchronized (commitWatch) {
      if (currentCommit.getId() == commitId)
        commitWatch.wait(timeOutMillis);
    }
  }
  
  
  
  @Override
  public String toString() {
    CommitRecord c = currentCommit;
    StringBuilder string = new StringBuilder()
      .append('[').append(name());
    if (c != null) {
      string.append(':').append(c.getId()).append(':').append(c.getTableIds());
    }
    return string.append(']').toString();
  }

}
