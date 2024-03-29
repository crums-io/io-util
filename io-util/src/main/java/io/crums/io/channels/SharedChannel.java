/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.io.channels;


import java.lang.System.Logger.Level;
import java.nio.channels.Channel;
import java.util.Objects;

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
    sharedCounter = new int[] { 1 };
    this.resource = Objects.requireNonNull(resource);
  }
  
  /**
   * Creates a <em>shared</em> instance.
   */
  public SharedChannel(SharedChannel copy) {
    this.sharedCounter = copy.sharedCounter;
    this.resource = copy.resource;
    
    synchronized (sharedCounter) {
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
    closed = true;
  }

  protected void handleClosingException(Exception x) {
    System.getLogger(SharedChannel.class.getName()).log(
        Level.WARNING,
        "Error ignored on closing " + resource + ": " + x);
  }

}
