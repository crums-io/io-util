/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.io.ipl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Objects;

import io.crums.io.FileUtils;
import io.crums.io.NumberedFileScheme;
import io.crums.io.channels.ChannelUtils;
import io.crums.util.RandomId;
import io.crums.util.TaskStack;

/**
 * <p>
 * Interprocess, file-based, locking protocol. This leverages the file system (whether local or
 * a shared remote) and builds on the property (assumption) that renaming (a.k.a. moving) a file is
 * an all-or-nothing operation. It does not assume however that everyone's view of the filesystem
 * is necessarily synchronous otherwise.
 * </p>
 * <h3>Outline of Protocol</h3>
 * <p>
 * At any one moment, the state of the lock is represented by a numbered lock file.
 * The lock file encodes 2 pieces of informaton:
 * <ol>
 * <li>
 * The owner of the lock file. The owner of the lock file is the only one allowed to modify it.
 * (And then only under certain constraints.)
 * </li><li>
 * The expiration time in epoch milliseconds. Ownership of the <em>lock</em> (not the file
 * lock file itself) is lost after expiration.
 * </li>
 * </ol>
 * Excepting updates by the owner of the lock file (more on this below), the only other way 
 * the state of the lock mutates is with the creation the next numbered lock file. The
 * rules for when and how a new lock file may be created are as follows:
 * </p><p>
 * <ol>
 * <li>
 * The expiration time recorded in the last numbered lock file must have already expired.
 * </li><li>
 * </li>
 * </ol>
 * </p><p>
 * TODO: old lock file delete (everyone's a deleter).
 * </p>
 */
public class FilebasedLock implements InterProcLock {
  
  /**
   * Lock file length. This is a <em>minimum</em>: this class ignores data beyond this offset.
   */
  public final static int LOCK_FILE_LENGTH = RandomId.LENGTH + 8;
  
  /**
   * Staging directory name.
   */
  public final static String STAGING = "staging";
  
  /**
   * Default extenstion. Stands for inter-process lock. (Unlikely to be confused with GTA files.)
   */
  public final static String DEFAULT_EXT = ".ipl";
  
  
  /**
   * Minimum argument value for {@linkplain #attempt(int)} is 100 milliseconds.
   */
  public final static int MIN_HOLD_MILLIS = 100;
  
  
  /**
   * Maximum argument value for {@linkplain #attempt(int)} is 30 seconds.
   */
  public final static int MAX_HOLD_MILLIS = 30_000;
  
  
  /**
   * If less than this many milliseconds remain before expiration, then
   * the expiration cannot be increased.
   * 
   * @see #updateExpiration(int)
   */
  public final static int FENCE_MILLIS_BEFORE_EXPIRATION_BOOST = 100;
  
  private final static int LATENCY_MILLIS = 10;
  
  
  
  
  
  private final File lockDir;
  private final File staging;
  private final NumberedFileScheme numberScheme;
  
  
  /**
   * Creates a new instance with the default filename extension.
   * 
   * @param lockDir non-null. If it doesn't already exist, on return it's created
   * 
   * @see #DEFAULT_EXT
   */
  public FilebasedLock(File lockDir) {
    this(lockDir, DEFAULT_EXT);
  }
  
  /**
   * Full parameter constructor. Creates or loads the lock file. If <em>created</em>,
   * then the lock will be already expired.
   * 
   * @param lockDir non-null. If it doesn't already exist, on return it's created
   * @param ext non-null. Include the dot if you want one (!)
   */
  public FilebasedLock(File lockDir, String ext) {
    this.lockDir = Objects.requireNonNull(lockDir, "null lockDir");
    this.staging = new File(lockDir, STAGING);
    
    this.numberScheme = NumberedFileScheme.withExt(ext);

    FileUtils.ensureDir(staging);
    
    if (numberScheme.longEntries(lockDir).isEmpty()) {
      File staged = newStagedFile(0);
      ByteBuffer buffer = ByteBuffer.allocate(LOCK_FILE_LENGTH).put(owner().id()).putLong(0).flip();
      FileUtils.writeNewFile(staged, buffer);
      FileUtils.moveOrDelete(staged, lockPath(0));
    }
  }
  
  
  
  /**
   * Determines whether the lock is owned (held) by <em>anyone</em>.
   */
  @Override
  public boolean isLocked() {
    ByteBuffer contents = loadLock();
    return System.currentTimeMillis() < expiration(contents);
  }
  
  
  /**
   * Determines whether the lock is owned (held) by <em>this process</em>.
   */
  @Override
  public boolean isOwner() {
    ByteBuffer contents = loadLock();
    return isOwner(contents) && System.currentTimeMillis() < expiration(contents);
  }
  
  
  private boolean isOwner(ByteBuffer contents) {
    boolean yes = owner().id().equals(contents.limit(RandomId.LENGTH));
    contents.clear();
    return yes;
  }
  
  
  /**
   * 
   * @see #FENCE_MILLIS_BEFORE_EXPIRATION_BOOST
   */
  @Override
  public boolean updateExpiration(int holdMillis) {
    if (holdMillis > MAX_HOLD_MILLIS)
      throw new IllegalArgumentException(
          "holdMillis must be <= " + MAX_HOLD_MILLIS + "; actual given: " + holdMillis);
    
    holdMillis = Math.max(0, holdMillis);
    
    long lastNumber = lastLockNumber();
    ByteBuffer contents = loadLock(lastNumber);
    
    if (!isOwner(contents))
      return false;
    
    long expiration = expiration(contents);
    long now = System.currentTimeMillis();
    long remainingMillis = expiration - now;
    
    if (remainingMillis <= LATENCY_MILLIS)  // (the stuff we do will easily take over 10 milliseconds)
      return false;

    long newExpiration = now + holdMillis;
    
    if (newExpiration > expiration && remainingMillis < FENCE_MILLIS_BEFORE_EXPIRATION_BOOST)
      return false;
    
    contents.putLong(RandomId.LENGTH, newExpiration);
    
    try (TaskStack closer = new TaskStack()) {
      File lockFile = lockPath(lastNumber);
      @SuppressWarnings("resource")
      FileChannel ch = new RandomAccessFile(lockFile, "rw").getChannel();
      closer.pushClose(ch);
      ChannelUtils.writeRemaining(ch, 0, contents.clear());
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
    
    return true;
  }
  
  
  
  @Override
  public boolean attempt(int holdMillis) {
    if (holdMillis < MIN_HOLD_MILLIS || holdMillis > MAX_HOLD_MILLIS)
      throw new IllegalArgumentException("holdMillis " + holdMillis);
    
    long lastNumber = lastLockNumber();
    ByteBuffer contents = loadLock(lastNumber);
    
    long expiration = expiration(contents);
    long now = System.currentTimeMillis();
    
    if (expiration - now > LATENCY_MILLIS)  // (the stuff we do will easily take 10 milliseconds)
      return false;
    
    long targetNumber = lastNumber + 1;
    
    ByteBuffer buffer = ByteBuffer.allocate(LOCK_FILE_LENGTH);
    buffer.put(owner().id()).putLong(now + holdMillis).flip();
    
    File staged = newStagedFile(targetNumber);
    FileUtils.writeNewFile(staged, buffer);
    
    File target = lockPath(targetNumber);
    
    return FileUtils.moveOrDelete(staged, target);
  }
  
  
  
  @Override
  public long expiration() {
    ByteBuffer contents = loadLock();
    return expiration(contents);
  }
  
  
  
  

  
  /**
   * Returns the last lock number. Informational: of no practical use to the user.
   */
  public long lastLockNumber() {
    List<Long> lockNumbers = numberScheme.longEntries(lockDir);
    int size = lockNumbers.size();
    assert size > 0;
    return lockNumbers.get(size - 1);
  }
  
  
  
  
  private RandomId owner() {
    return RandomId.RUN_INSTANCE;
  }
  
  
  private File lockPath(long lockNumber) {
    return new File(lockDir, numberScheme.toFilename(lockNumber));
  }
  
  
  private File newStagedFile(long lockNumber) {
    return new File(staging, owner() + "-" + lockNumber + "-T" + System.currentTimeMillis());
  }
  
  private long expiration(ByteBuffer lockContents) {
    return lockContents.getLong(RandomId.LENGTH);
  }
  
  
  /**
   * Loads and returns the contents of the last lock file.
   */
  private ByteBuffer loadLock() {
    long lockNumber = lastLockNumber();
    return loadLock(lockNumber);
  }
  
  
  private ByteBuffer loadLock(long lockNumber) throws UncheckedIOException {
    File lockFile = lockPath(lockNumber);
    return FileUtils.loadFileToMemory(lockFile);
  }
}
