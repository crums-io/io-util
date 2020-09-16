/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.io.ipl;


import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import io.crums.io.FileUtils;
import io.crums.io.channels.ChannelUtils;

/**
 * On disk backing table for implementng an exclusive lock acquisition (and release) protocol across processes. The
 * word <em>process</em> here is to be understood in the most liberal sense: these may not live
 * in the same operating system. Rather, this is designed to also work, for example, on a shared
 * file system.
 * <p>
 * 
 * </p>
 * 
 */
public class LockTable implements Closeable {
  

  /**
   * Min number of cells (inclusive).
   */
  public final static int MIN_CELL_COUNT = 4;
  
  /**
   * Max number of cells (inclusive).
   */
  public final static int MAX_CELL_COUNT = 64;
  
  public final static int TIME_HEADER_SIZE = 16;
  public final static int ROW_WIDTH = 32;
  
  private final static byte[] ZERO_ROW = new byte[ROW_WIDTH];
  
  private final static String USERID_DIGEST_ALGO = "SHA-256";
  
  
  static class LockState {
    
    // never change its state!
    private final static ByteBuffer ZERO_BUFFER = ByteBuffer.wrap(ZERO_ROW).asReadOnlyBuffer();
    
    private final ByteBuffer state;
    
    LockState(ByteBuffer state) {
      this.state = state.asReadOnlyBuffer().clear();
    }
    
    boolean available() {
      long now = System.currentTimeMillis();
      return false;
    }
    
    public long startTime() {
      return state.getLong(0);
    }
    
    public long expiration() {
      return state.getLong(8);
    }
    
    public boolean isHeld() {
      return expiration() > System.currentTimeMillis();
    }
    
    public int cellCount() {
      return (state.capacity() - TIME_HEADER_SIZE) / ROW_WIDTH;
    }
    
    ByteBuffer ownerId() {
      final ByteBuffer rows = userIdRows();
      final ByteBuffer id = rows.limit(ROW_WIDTH).slice();
      
      rows.position(ROW_WIDTH);
      
      while (rows.position() < rows.capacity()) {
        int limit = rows.position() + ROW_WIDTH;
        rows.limit(limit);
        if (!id.equals(rows))
          return null;
        rows.position(limit);
      }
      return id;
    }
    
    
    
    int countOwnedRows(ByteBuffer userId) {
      assert userId.remaining() == ROW_WIDTH;
      final ByteBuffer rows = userIdRows();
      int count = 0;
      while (rows.position() < rows.capacity()) {
        int limit = rows.position() + ROW_WIDTH;
        rows.limit(limit);
        if (userId.equals(rows))
          ++count;
        rows.position(limit);
      }
      return count;
    }
    
    
    
    List<OwnerStats> ownerStats() {
      
      HashMap<ByteBuffer, OwnerStats> map = new HashMap<>(cellCount());
      {
        final ByteBuffer rows = userIdRows();
        do {
          
          rows.limit(rows.position() + ROW_WIDTH);
          // zero rows don't count: they exist as hints that a row has been given up
          if (ZERO_BUFFER.equals(rows))
            continue;
          
          OwnerStats stats = map.get(rows);
          
          if (stats == null) {
            
            stats = new OwnerStats(rows); // (rows was sliced)
            map.put(stats.userId(), stats);
          
          } else
            stats.incrementRowsOwned();
          
          rows.position(rows.limit());
        
        } while (rows.position() < rows.capacity());
      }
      ArrayList<OwnerStats> allStats = new ArrayList<>(map.values());
      Collections.sort(allStats);
      return allStats;
    }
    
    
    
    /**
     * Returns a new, independent view onto the userId rows.
     */
    ByteBuffer userIdRows() {
      return state.duplicate().position(TIME_HEADER_SIZE).slice();
    }
    
  }
  
  
  /**
   * Owner stats, ranked from most rows owned to least. Not suitable as a key in a map
   * since it has mutable state.
   */
  private static class OwnerStats implements Comparable<OwnerStats> {
    private final ByteBuffer userId;
    private int rowsOwned = 1;
    
    OwnerStats(ByteBuffer userId) {
      this.userId = userId.slice();
    }
    
    void incrementRowsOwned() {
      ++rowsOwned;
    }
    
    int rowsOwned() {
      return rowsOwned;
    }
    
    ByteBuffer userId() {
      return userId;
    }

    @Override
    public int compareTo(OwnerStats o) {
      int comp = o.rowsOwned - rowsOwned;
      return comp == 0 ? userId.compareTo(o.userId) : comp;
    }
    
    
    public boolean equals(Object o) {
      if (o == this)
        return true;
      else if (o instanceof OwnerStats) {
        OwnerStats stats = (OwnerStats) o;
        return userId.equals(stats.userId) && rowsOwned == stats.rowsOwned;
      } else
        return false;
    }
    
    
    public int hashCode() {
      return userId.hashCode() ^ rowsOwned;
    }
    
    
  }
  
  
  
  
  
  
  private final Random random = new Random();
  private final MessageDigest digester;

  private final byte[] userId = new byte[ROW_WIDTH];
  
  private final ByteBuffer fileState;
  private final LockState state;
  private final LockPolicy policy;
  
  private final File file;
  
  private FileChannel channel;
  
  
  /**
   * Creates a new file containing the lock table. The lock is initialized as unowned with
   * random user IDs.
   * 
   * @param file    the target file (must not exist)
   * @param policy  lock timing policies
   * @param cells   the number of user ID cells
   */
  public LockTable(File file, LockPolicy policy, int cells) throws IOException {
    this.file = Objects.requireNonNull(file, "null file");
    this.policy = Objects.requireNonNull(policy, "null policy");
    
    if (file.isFile())
      throw new IllegalArgumentException("will not overwrite existing file " + file);
    
    if (cells < MIN_CELL_COUNT || cells > MAX_CELL_COUNT)
      throw new IllegalArgumentException("cells outside range: " + cells);
    
    this.fileState = ByteBuffer.allocate(TIME_HEADER_SIZE + cells * ROW_WIDTH);
    this.state = new LockState(fileState);
    
    // Randomize the contents, and make sure it's expired
    // --by setting the time fields to zero
    
    fileState.position(TIME_HEADER_SIZE);
    while(fileState.hasRemaining())
      fileState.putLong(random.nextLong());
    fileState.clear();
    
    FileUtils.ensureDir(file.getParentFile());
    
    ChannelUtils.writeRemaining(getChannel(), 0, fileState);
    fileState.clear();
    
    this.digester = newDigester();
  }

  /**
   * Loads an existing lock table.
   * 
   * @param file    the target file (must already exist)
   * @param policy  lock timing policies
   */
  public LockTable(File file, LockPolicy policy) throws IOException {
    this.file = Objects.requireNonNull(file, "null file");
    this.policy = Objects.requireNonNull(policy, "null policy");
    
    if (!file.isFile())
      throw new IllegalArgumentException("not an existing file: " + file);
    
    {
      long rowspace = file.length() - TIME_HEADER_SIZE;
      
      long rows = rowspace / ROW_WIDTH;
      if (rows < MIN_CELL_COUNT || rows > MAX_CELL_COUNT)
        throw new IllegalArgumentException("file length (" + file.length() + ") outside valid bounds: " + file);
      if (rowspace % ROW_WIDTH != 0)
        throw new IllegalArgumentException("file length (" + file.length() + ") does not end at cell boundary: " + file);
    }
    this.fileState = ByteBuffer.allocate((int) file.length());
    this.state = new LockState(fileState);
    loadState();
    
    this.digester = newDigester();
  }
  
  
  
  
  
  @Override
  public synchronized void close() throws IOException {
    if (channel != null) {
      channel.close();
      channel = null;
    }
    clearUserId();
  }
  
  
  
  public synchronized boolean acquire(long period, long timeout) throws IOException, InterruptedException {
//    if (isHeld())
//      throw new IllegalStateException("lock is already held");
    
    loadState();
    long now = System.currentTimeMillis();
    if (state.startTime() - now > policy.getTimeAssertionFlack())
      throw new LockProtocolException("startTime=" + state.startTime() + "; now=" + now + "; policy=" + policy);
    long expiration = state.expiration();
    
    
    
    
    return false;
  }
  
  
//  public boolean isHeld() {
//    if (!isUserIdSet())
//      return false;
//    
//    return false;
//  }
  
  private boolean isUserIdSet() {
    int index = userId.length;
    while (index-- > 0 && userId[index] == 0);
    return index != -1;
  }
  
  private void clearUserId() {
    for (int index = userId.length; index-- > 0;  )
      userId[index] = 0;
  }
  
  
  private void loadState() throws IOException {
    fileState.clear();
    ChannelUtils.readRemaining(getChannel(), 0, fileState);
    fileState.flip();
  }
  
  
  /**
   * Returns a read-write channel to the lock file, opening it as necessary.
   * 
   * @see #close();
   */
  FileChannel getChannel() throws IOException {
    if (channel == null || !channel.isOpen()) {
      channel = new RandomAccessFile(file, "rw").getChannel();
    }
    return channel;
  }
  
  
  private MessageDigest newDigester() {
    try {
      return MessageDigest.getInstance(USERID_DIGEST_ALGO);
    } catch (NoSuchAlgorithmException nsax) {
      throw new AssertionError("algo: " + USERID_DIGEST_ALGO, nsax);
    }
  }

}
