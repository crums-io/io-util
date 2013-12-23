/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.util.cc;

import java.util.concurrent.ThreadFactory;

/**
 * 
 * @author Babak
 */
public class FixedPriorityThreadFactory implements ThreadFactory {
  
  private static int groupCounter;
  private static synchronized int newGroupNum() {
    return ++groupCounter;
  }
  
  private final ThreadGroup threadGroup;
  private int count;


  public FixedPriorityThreadFactory(int priority) {
    this(null, priority);
  }
  
  public FixedPriorityThreadFactory(ThreadGroup threadGroup, int priority) {
    int groupNum = newGroupNum();
    if (threadGroup == null) {
      ThreadGroup cg = Thread.currentThread().getThreadGroup();
      threadGroup = new ThreadGroup(
          cg, "[id=" + groupNum + ",priority=" + priority + "]");
    }
    this.threadGroup = threadGroup;
    threadGroup.setMaxPriority(priority);
  }


  @Override
  public Thread newThread(Runnable r) {
    Thread t = new Thread(threadGroup, r, Integer.toString(++count));
    t.setPriority(threadGroup.getMaxPriority());
    return t;
  }
  
  
  public int getMaxPriority() {
    return threadGroup.getMaxPriority();
  }
  
  

}
