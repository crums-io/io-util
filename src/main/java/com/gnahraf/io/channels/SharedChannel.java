/*
 * Copyright 2020 Babak Farhang
 */
package com.gnahraf.io.channels;


import java.nio.channels.Channel;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * Reference-counted resource closer.
 */
public class SharedChannel implements Channel {
  
  private final int[] sharedCounter;
  private final AutoCloseable resource;
  private boolean closed;

  
  /**
   * Creates a <em>first</em> instance.
   */
  public SharedChannel(AutoCloseable resource) {
    sharedCounter = new int[1];
    sharedCounter[0] = 1;
    this.resource = Objects.requireNonNull(resource);
  }
  
  /**
   * Creates a <em>shared</em> instance.
   */
  public SharedChannel(SharedChannel copy) {
    this.sharedCounter = copy.sharedCounter;
    this.resource = copy.resource;
    
    synchronized (sharedCounter) {
      assert sharedCounter[0] >= 0;
      if (sharedCounter[0] < 1)
        throw new IllegalStateException("resource is already closed");
      ++sharedCounter[0];
    }
  }

  @Override
  public boolean isOpen() {
    return !closed;
  }

  @Override
  public synchronized void close() {
    if (closed)
      return;
    
    synchronized (sharedCounter) {
      assert sharedCounter[0] > 0;
      if (--sharedCounter[0] == 0)
        try {
          resource.close();
        } catch (Exception x) {
          handleClosingException(x);
        }
    }
  }

  protected void handleClosingException(Exception x) {
    Logger.getLogger(SharedChannel.class.getName()).warning("exception on closing " + resource + ": " + x);
  }

}
