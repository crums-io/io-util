/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.main;

/**
 * Exit codes for {@linkplain System#exit(int)}.
 */
public enum StdExit {
  
  /**
   * Normal, successful program exit. 0.
   */
  OK(0),
  /**
   * Catch all. 1.
   */
  GENERAL_ERROR(1),
  /**
   * Illegal argument at the command line. Questionable consensus
   * about the exact meaning of <code>126</code>, but adopting this
   * semantic. 126.
   */
  ILLEGAL_ARG(126),
  /**
   * Exit by interrupt signal. Assume as if by <code><em>kill -9 &lt;JVM_PID&gt;</em></code>.
   * 137 (<code>= 128 + 9</code>)
   */
  INTERRUPTED(137);
  
  
  /**
   * The exit code.
   */
  public final int code;
  
  private StdExit(int code) {
    this.code = code;
  }
  
  
  public boolean error() {
    return code != 0;
  }
  
  
  public boolean ok() {
    return code == 0;
  }
  
  
  /**
   * Exits the Java process with this exit code. (Obviously, a dangerous
   * method. But then so is {@code System.exit}.)
   */
  public void exit() {
    System.exit(code);
  }

}
