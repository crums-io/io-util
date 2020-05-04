/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.util;


import java.nio.channels.Channel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;

/**
 * Utility for resource releasing.  Mostly, this has to do
 * with the fact that exception-safe code in Java ends up with a clutter
 * of try/finally clauses.  (Language envy: C++ shines here..)
 * <p/>
 * <h3>Example usage</h3>
 * <h4>Java 1.6-</h4>
 * <p/>
 * <pre>
 *      TaskStack closer = new TaskStack();
 *      try {
 *          Lock a = ..
 *          a.lock();
 *          closer.pushUnlock(a);
 *          Lock b = ..
 *          
 *          if (!b.tryLock())
 *              return;
 *           
 *          FileChannel file = openFile();
 *          closer.pushClose(file);
 *          
 *          // do some work
 *          ..
 *      } finally {
 *          closer.close();
 *      }
 * </pre>
 * <p/>
 * <h4>Java 1.7+</h4>
 * <p/>
 * <pre>
 *      try (TaskStack closer = new TaskStack()) {
 *          Lock a = ..
 *          a.lock();
 *          closer.pushUnlock(a);
 *          Lock b = ..
 *          
 *          if (!b.tryLock())
 *              return;
 *           
 *          FileChannel file = openFile();
 *          closer.pushClose(file);
 *          
 *          // do some work
 *          ..
 *      }
 * </pre>
 * 
 * 
 * @author Babak
 */
public class TaskStack implements Channel {
  
  protected final List<AutoCloseable> opStack = Collections.synchronizedList(new ArrayList<AutoCloseable>());
  protected final Logger log;
  
  
  public TaskStack() {
    this.log = Logger.getLogger(getClass().getName());
  }
  
  /**
   * Creates an instance using the given <tt>user</tt> object's class to determine
   * the logger.
   * 
   * @param user the user object, or <tt>null</tt>
   */
  public TaskStack(Object user) {
    Class<?> logClass = user == null ? getClass() : user.getClass();
    this.log = user instanceof Logger ? (Logger) user : Logger.getLogger(logClass.getName());
  }
  
  public TaskStack(Logger log) {
    this.log = log == null ? Logger.getLogger(getClass().getName()) : log;
  }
  
  
  public TaskStack pushRun(final Runnable task) {
    if (task == null)
      throw new IllegalArgumentException("null task");
    
    AutoCloseable asClose = new AutoCloseable() {
      @Override
      public void close() {
        task.run();
      }
    };
    opStack.add(asClose);
    return this;
  }
  
  
  public TaskStack pushClose(AutoCloseable resource) {
    if (resource == null)
      throw new IllegalArgumentException("null resource");
    opStack.add(resource);
    return this;
  }
  
  
  public TaskStack pushClose(AutoCloseable... resource) {
    for (AutoCloseable r : resource)
      pushClose(r);
    return this;
  }
  
  public TaskStack pushClose(Iterable<? extends AutoCloseable> resources) {
    for (AutoCloseable r : resources)
      pushClose(r);
    return this;
  }
  
  
  public TaskStack pushUnlock(final Lock lock) {
    if (lock == null)
      throw new IllegalArgumentException("null lock");
    
    AutoCloseable asClose = new AutoCloseable() {
      @Override
      public void close() {
        lock.unlock();
      }
    };
    opStack.add(asClose);
    return this;
  }
  
  /**
   * Pops <tt>count</tt> many times and returns the result.
   * 
   * @param count only meaningful if &gt; 0
   * @return
   */
  public int pop(int count) {
    while (count-- > 1)
      pop();
    return pop();
  }
  
  /**
   * Pops the last pushed operation from the stack and executes (closes) it.
   *  
   * @return the remaining number of operations on the stack, or -1 if the stack was empty
   */
  public int pop() {
    
    final int removedIndex;
    AutoCloseable resource;
    synchronized (opStack) {
      removedIndex = opStack.size() - 1;
      if (removedIndex == -1)
        return -1;
      resource = opStack.remove(removedIndex);
    }
    
    try {
      resource.close();
    } catch (Exception x) {
      log.severe(
          "On closing resource[" + removedIndex + "] ("  + resource + "): "+ x.getMessage());
    }
    return removedIndex;
  }


  /**
   * Unwinds the stack.
   */
  @Override
  public void close() {
    while (pop() > 0);
  }
  
  /**
   * Returns the number of operations remaining on the stack.
   */
  public int size() {
    return opStack.size();
  }

  
  /**
   * Tells whether there are any operations remaining on the stack.
   */
  @Override
  public boolean isOpen() {
    return !opStack.isEmpty();
  }

  
  /**
   * Clears the stack. If there are any operations on the stack, they are discarded (ignored).
   * This comes handy when the <tt>TaskStack</tt> is a clean-up-on-failure type of construct.
   * For example,
   * <pre>
   * {@code
   * 
    try (TaskStack closeOnFail = new TaskStack()) {
      InputStream in = ..
      closeOnFail.pushClose(in);
       .
       .
      closeOnFail.pushClose(sock);
       .
       .
      // if we get this far, we're all good..
      closeOnFail.clear();
    }
   * 
   * }
   * </pre>
   */
  public TaskStack clear() {
    opStack.clear();
    return this;
  }
  
}
