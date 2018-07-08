/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.karoon;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.gnahraf.io.Files;
import com.gnahraf.io.IoStateException;
import com.gnahraf.io.buffer.Covenant;
import com.gnahraf.io.store.karoon.merge.TableMergeEngine;
import com.gnahraf.io.store.ks.CachingKeystone;
import com.gnahraf.io.store.ks.Keystone;
import com.gnahraf.io.store.table.TableSet;
import com.gnahraf.math.stats.MovingAverage;
import com.gnahraf.util.TaskStack;
import com.gnahraf.util.cc.throt.FuzzySpeed;
import com.gnahraf.util.cc.throt.FuzzyThrottler;

/**
 * Single thread access table store.
 * 
 * @author Babak
 */
public class TStore implements Channel {
  
  public class TmeContext {
    
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
  
  
  private WriteAheadTableBuilder writeAhead;
  private SidTableSet activeTableSet;
  private volatile CommitRecord currentCommit;
  
  private final Object commitWatch = new Object();
  
  
  public TStore(TStoreConfig config, boolean create) throws IOException {
    if (config == null)
      throw new IllegalArgumentException("null config");
    this.config = config;
    boolean failed = true;
    try {
      if (create)
        Files.ensureDir(config.getRootDir());
      else
        Files.assertDir(config.getRootDir());
      
      File counterFile = new File(config.getRootDir(), COUNTERS_FILENAME);
      if (counterFile.exists()) {
        Files.assertFile(counterFile);
        FileChannel file = new RandomAccessFile(counterFile, "rw").getChannel();
        file.position(0);
        tableCounter = new CachingKeystone(Keystone.loadInstance(file));
        closer.pushClose(tableCounter);
        commitNumber = new CachingKeystone(Keystone.loadInstance(file));
        walTableNumber = new CachingKeystone(Keystone.loadInstance(file));
        
      } else if (create) {
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
          Files.delete(sortedTableFile);
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
    Files.assertDoesntExist(writeAheadFile);
    writeAhead = new WriteAheadTableBuilder(
        config.getRowWidth(), config.getRowOrder(), writeAheadFile);
    walTableNumber.set(walTableId);
  }
  
  
  
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
    
    if (row != null && config.getDeleteCodec().isDeleted(row))
      return null;
    else
      return row;
  }

  
  
  public void deleteRow(ByteBuffer key) throws IOException {
    deleteRow(key, true);
  }
  
  
  public void deleteRow(ByteBuffer key, boolean checkExists) throws IOException {

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
  
  
  public void setRow(ByteBuffer row) throws IOException {
    setRow(row, Covenant.NONE);
  }
  
  
  public void setRow(ByteBuffer row, Covenant promise) throws IOException {
    synchronized (apiLock) {
      writeAhead.putRow(row, promise);
      manageWriteAhead();
    }
  }
  
  /**
   * Inserts or updates the given <tt>rows</tt>. The rows are passed in <em>en bloc</em>:
   * they may be ordered in any way. The operation is fail-safe (all-or-nothing).
   * <p/>
   * Pay attention to the <tt>promise</tt> parameter. It has a huge impact on performance,
   * but just as importantly, if you break the promise, you break the data set.
   * 
   * @param rows
   *        the remaining bytes in this buffer represent the rows being input. The
   *        remaining bytes must be an exact multiple of the
   *        {@linkplain TStoreConfig#getRowWidth() row width}. If the
   *        same row (recall, the identity of a row is defined by the table's
   *        {@linkplain RowOrder}) occurs twice in this buffer, then the last occurance
   *        wins. 
   * @param promise
   *        declares how and whether the caller <em>agrees not to later modify</em> the
   *        given <tt>rows</tt> parameter. <em><strong>If the caller breaks the promise, then
   *        the table will almost certainly get corrupted!</strong></em>. May be <tt>null</tt>
   *        (no promise), but that would be a shame.
   */
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
      Files.assertFile(file);
    else
      Files.assertDoesntExist(file);
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
  
  
  private SidTable loadSortedTable(File tableFile, long id) throws IOException {
    FileChannel ch = new RandomAccessFile(tableFile, "r").getChannel();
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
      .append('[').append(config.getRootDir().getName());
    if (c != null) {
      string.append(':').append(c.getId()).append(':').append(c.getTableIds());
    }
    return string.append(']').toString();
  }

}
