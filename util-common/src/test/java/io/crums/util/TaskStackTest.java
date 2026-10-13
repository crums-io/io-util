/*
 * Copyright 2013-2022 Babak Farhang
 */
package io.crums.util;

import static org.junit.jupiter.api.Assertions.*;


import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class TaskStackTest {
  
  private ArrayList<Integer> unwindOutput = new ArrayList<Integer>();

  @Test
  public void testEmpty() {
    @SuppressWarnings("resource")
    TaskStack stack = new TaskStack();
    assertEquals(-1, stack.pop());
  }

  @Test
  public void test01AutoCloseable() {
    final int expectedCount = 5;
    try (TaskStack work = new TaskStack()) {
      for (int i = expectedCount; i-- > 0;)
        pushTestClose(work);
    }
    verifyTest(expectedCount);
  }

  @Test
  public void test02Runnable() {
    final int expectedCount = 5;
    try (TaskStack work = new TaskStack()) {
      for (int i = expectedCount; i-- > 0;)
        pushTestRun(work);
    }
    verifyTest(expectedCount);
  }

  @Test
  public void test03Unlock() {
    final int expectedCount = 5;
    try (TaskStack work = new TaskStack()) {
      for (int i = expectedCount; i-- > 0;)
        pushTestUnlock(work);
    }
    verifyTest(expectedCount);
  }

  @Test
  public void test04Mixed() {
    final int expectedCount = 3;
    try (TaskStack work = new TaskStack()) {
      pushTestUnlock(work);
      pushTestRun(work);
      pushTestClose(work);
    }
    verifyTest(expectedCount);
  }
  
  
  private void verifyTest(int expectedCount) {
    assertEquals(expectedCount, unwindOutput.size());
    for (int i = 0, j = unwindOutput.size(); j-- > 0; ++i)
      assertEquals(i, unwindOutput.get(j).intValue());
  }
  
  
  private void pushTestClose(TaskStack stack) {
    final Integer index = stack.size();
    AutoCloseable testClose = new AutoCloseable() {
      @Override
      public void close() throws Exception {
        unwindOutput.add(index);
      }
    };
    stack.pushClose(testClose);
  }
  
  
  private void pushTestRun(TaskStack stack) {
    final Integer index = stack.size();
    Runnable testRun = new Runnable() {
      @Override
      public void run() {
        unwindOutput.add(index);
      }
    };
    stack.pushRun(testRun);
  }
  
  
  private void pushTestUnlock(TaskStack stack) {
    final Integer index = stack.size();
    Lock testLock = new Lock() {
      @Override
      public void unlock() {
        unwindOutput.add(index);
      }
      @Override
      public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
      }
      @Override
      public boolean tryLock() {
        return false;
      }
      @Override
      public Condition newCondition() {
        return null;
      }
      @Override
      public void lockInterruptibly() throws InterruptedException {
      }
      @Override
      public void lock() {
      }
    };
    stack.pushUnlock(testLock);
    
  }

}
