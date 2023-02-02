/*
 * Copyright 2013 Babak Farhang 
 */
package io.crums.util;


import java.lang.System.Logger.Level;
import java.nio.channels.Channel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

/**
 * <p>
 * Utility for resource releasing.  Mostly, this has to do
 * with the fact that exception-safe code in Java ends up with a clutter
 * of try/finally clauses.  (Language envy: C++ shines here..)
 * </p>
 * <h2>Example usage</h2>
 * <p></p>
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
 *          closer.pushUnlock(b);
 *           
 *          FileChannel file = openFile();
 *          closer.pushClose(file);
 *          
 *          // do some work
 *          ..
 *      }
 * </pre>
 */
public class TaskStack implements Channel {
  
  protected final List<AutoCloseable> opStack = Collections.synchronizedList(new ArrayList<AutoCloseable>());
  
  
  
  
  /**
   * Pushes a {@code Runnable} onto the stack, to be run when the stack
   * unwinds (closes).
   * 
   * @param task the task to be run on closing
   * @return {@code this}
   */
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
  
  
  public TaskStack pushCall(Callable<?> task) {
    Objects.requireNonNull(task, "null task");
    AutoCloseable asClose = new AutoCloseable() {
      @Override
      public void close() {
        try {
          task.call();
        } catch (Exception x) {
          throw new RuntimeException("on closing " + task, x);
        }
      }
      @Override
      public String toString() {
        return "AutoClosedCallable<" + task + ">";
      }
    };
    opStack.add(asClose);
    return this;
  }
  
  
  /**
   * Pushes a {@code resource} onto the stack, to be closed when the stack
   * unwinds (closes).
   * 
   * @param resource the resource to be closed on closing
   * @return {@code this}
   */
  public TaskStack pushClose(AutoCloseable resource) {
    if (resource == null)
      throw new IllegalArgumentException("null resource");
    if (resource == this)
      throw new IllegalArgumentException("circular close detected");
    opStack.add(resource);
    return this;
  }
  
  

  /**
   * Pushes one or more {@code resource}s onto the stack, to be closed when the stack
   * unwinds (closes).
   * 
   * @param resource the resource[s] to be closed on closing
   * @return {@code this}
   */
  public TaskStack pushClose(AutoCloseable... resource) {
    for (AutoCloseable r : resource)
      pushClose(r);
    return this;
  }
  
  
  /**
   * Pushes zero or more {@code resources} onto the stack, to be closed when the stack
   * unwinds (closes).
   * 
   * @param resources bunch of resources (not null but may be empty) to be unlocked
   *                  (in the reverse order of the iteration, since this is a stack)
   *                  when the instance closes
   * @return {@code this}
   */
  public TaskStack pushClose(Iterable<? extends AutoCloseable> resources) {
    for (AutoCloseable r : resources)
      pushClose(r);
    return this;
  }
  
  
  /**
   * Pushes a {@code lock} onto the stack, to be unlocked when the stack unwinds (closes).
   * 
   * @param lock its {@linkplain Lock#unlock()} method to called when the instance closes
   * @return {@code this}
   */
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
   * Pops <code>count</code> many times and returns the result.
   * 
   * @param count only meaningful if &gt; 0
   * @return the return value from the last {@linkplain #pop() pop}
   */
  public int pop(int count) {
    while (count-- > 1)
      pop();
    return pop();
  }
  
  /**
   * Pops the last pushed operation from the stack and executes (closes) it.
   * Any error on encountered is simply logged and ignored.
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
      onCloseError(removedIndex, resource, x);
    }
    return removedIndex;
  }
  
  
  /**
   * Writes an error message to std err. No longer logged (so as to remove dependency on
   * logging and aid modularization.
   * 
   * @param removedIndex
   * @param resource
   * @param x
   */
  protected void onCloseError(int removedIndex, AutoCloseable resource, Exception x) {
    System.getLogger(TaskStack.class.getSimpleName()).log(
        Level.ERROR,
        "On closing resource[" + removedIndex + "] ("  + resource + "): "+ x.getMessage());
  }


  /**
   * Unwinds the stack, closing (or running, unlocking, etc.) every resource
   * as it does. As if invoking {@code while (pop() > 0);} directly.
   * 
   * @see #pop()
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
   * This comes handy when the <code>TaskStack</code> is a clean-up-on-failure type of construct.
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
