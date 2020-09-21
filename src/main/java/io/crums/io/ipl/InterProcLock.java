/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.io.ipl;

/**
 * 
 */
public interface InterProcLock {

  /**
   * Determines whether the lock is owned (held) by <em>anyone</em>.
   */
  boolean isLocked();

  /**
   * Determines whether the lock is owned (held) by <em>this process</em>.
   */
  boolean isOwner();

  /**
   * Updates the expiration to <em>system time</em> plus <tt>holdMillis</tt>. The caller
   * must already own the lock for the call to succeed. There must also be suffient time to
   * expiration.
   * 
   * @return <tt>true</tt> iff the lock was owned and its expiration was updated.
   */
  boolean updateExpiration(int holdMillis);

  /**
   * Attempts to acquire the lock for the given holding period.
   * 
   * @param holdMillis the holding period in milliseconds
   * 
   * @return <tt>true</tt> iff the lock was acquired
   */
  boolean attempt(int holdMillis);

  /**
   * Returns the current expiration date of the lock in UTC millis.
   */
  long expiration();

  /**
   * Returns the time remaining to expiration; 0, if expired.
   * 
   * @return millis
   */
  default long remaingTime() {
    long expiration = expiration();
    long now = System.currentTimeMillis();
    return now < expiration ? expiration - now : 0;
  }

  
  

}
