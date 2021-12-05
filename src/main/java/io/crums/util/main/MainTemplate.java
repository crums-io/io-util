/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.main;


import static io.crums.util.main.Args.*;

import java.io.PrintStream;

import io.crums.util.UnanonymousType;

/**
 * If many console programs start to look alike, well you try..
 * <p>
 * All the methods in this class are <tt>protected</tt> because they
 * do dangerous things--like exit the program. 
 * </p>
 * 
 * @see ArgList
 */
public abstract class MainTemplate extends UnanonymousType {
  
  public final static String DEBUG = "debug";
  public final static String DEBUG_CMD = DEBUG + "=true";
  
  protected final static String ERROR_TAG = "[ERROR] ";
  

  
  
  /**
   * Launch point handling help and errors. The model is as follows:
   * <ol>
   * <li>If there's a <tt>-help</tt> command in the argument list, then
   *     {@linkplain #printHelpAndExit()} is invoked and the program exits.
   * </li><li>
   *     Next, {@linkplain #init(String[])} is invoked. {@linkplain IllegalArgumentException}s
   *     thrown by this method are treated as user-input error and invoked
   *     {@linkplain #exitInputError(String)} which terminates the program.
   * </li><li>
   *     Finally, {@linkplain #start()} gets invoked. Here, <tt>Exception</tt>s raised are generally
   *     treated as <em>program</em> error. {@linkplain InterruptedException}s however are
   *     special and are used as interrupt handling.
   * </li>
   * </ol>
   * 
   * @param args  the arguments passed in from static main
   * 
   * @see ArgList
   */
  protected void doMain(String[] args) {
    if (help(args)) {
      printHelpAndExit();
      return;
    }
    try {
      
      try {
        
        init(args);
        
      } catch (IllegalArgumentException iax) {
        String msg = iax.getMessage();
        exitInputError(msg == null ? makeErrorMessage(iax) : msg);
        return;
      }
      
      start();
      
    } catch (InterruptedException ix) {
      
      close();
      StdExit.INTERRUPTED.exit();
    
    } catch (Exception e) {
      
      close();
      exitOnProgramError(e, args);
    }
  }
  
  /**
   * Initializes the instance. Program configuration occurs here. Note the exception semantics.
   * 
   * @param args                        arguments passed in from static main
   * @throws IllegalArgumentException   indicates a user input error detected early on
   *                                    (as with {@linkplain #getRequiredParam(String[], String)} for e.g.)
   * @throws Exception                  indicates a program failure; will exit after
   *      invoking {@linkplain #close()}
   */
  protected abstract void init(String[] args) throws IllegalArgumentException, Exception;
  
  protected ArgList newArgList(String[] args) {
    ArgList argList = new ArgList(args);
    argList.removeContained(DEBUG_CMD);
    return argList;
  }
  
  
  /**
   * Starts the program.
   * 
   * @throws InterruptedException if not handled before here, then the program exits with
   *    {@linkplain StdExit#INTERRUPTED} code
   * 
   * @throws Exception signals abnormal program termination; the program will exit after
   *      invoking {@linkplain #close()}
   */
  protected abstract void start() throws InterruptedException, Exception;
  
  /**
   * Exits the program on error (exit code 2).
   * 
   * @param e     the exception indicating the error condition
   * @param args  the arguments passed in from static main
   */
  protected void exitOnProgramError(Exception e, String[] args) {
    System.err.println();
    System.err.println();
    printError(makeErrorMessage(e));
    if (debugEnabled(args))
      e.printStackTrace(System.err);
    StdExit.GENERAL_ERROR.exit();
  }
  
  private String makeErrorMessage(Exception e) {
    String message = e.getMessage();
    return e.getClass().getSimpleName() + (message == null ? "" : (" "  + message));
  }
  
  protected boolean debugEnabled(String[] args) {
    return contains(args, DEBUG_CMD);
  }
  
  /**
   * Prints the given message on <tt>System.err</tt>, followed by an empty line.
   */
  protected void printError(String message) {
    System.err.println(ERROR_TAG + message);
  }
  
  /**
   * Exits the program after printing the given error <tt>message</tt>,
   * followed by the {@linkplain #printUsage(PrintStream)}. Exit status
   * code 126.
   * 
   * @see StdExit#ILLEGAL_ARG
   */
  protected void exitInputError(String message) {
    System.err.println();
    printError(message);
    System.err.println();
    System.err.println("Use -h (-help) to see required arguments.");
    StdExit.ILLEGAL_ARG.exit();
  }
  
  /**
   * Prints the help message and exits (status code 0).
   * 
   * @see #printDescription()
   * @see #printUsage(PrintStream)
   * @see #printLegend(PrintStream)
   */
  protected void printHelpAndExit() {
    printDescription();
    printUsage(System.out);
    printLegend(System.out);
    StdExit.OK.exit();
  }
  
  /**
   * Prints the description to <tt>System.out</tt>. Something like
   * <p><pre><tt>
    System.out.println();
    System.out.println("Description:");
    System.out.println();
    System.out.println("Outputs one or an ordered list of files managed under a directory.");
    System.out.println("This is a read only interface.");
    System.out.println();
    
   * </tt></pre></p>
   */
  protected abstract void printDescription();
  
  /**
   * Prints the usage. Something like
   * <p><pre><tt>
    out.println("Usage:");
    out.println();
    out.println("Arguments are specified as 'name=value' pairs.");
    out.println();
    TablePrint table = new TablePrint(out, 10, 65, 3);
    table.setIndentation(1);
    table.printRow(EXT + "=*", "the file extension (inc..
   * </tt></pre></p>
   */
  protected abstract void printUsage(PrintStream out);
  
  
  /**
   * Noop default.
   */
  protected void printLegend(PrintStream out) {
  }
  
  /**
   * Returns the value of the required <tt>name=value</tt> pair or if it doesn't exits
   * with {@linkplain #exitInputError(String)}.
   */
  protected String getRequiredParam(String[] args, String param) {
    String value = getValue(args, param, null);
    if (value == null || value.isEmpty())
      exitInputError("Missing required " + param + "={value} parameter");
    return value;
  }
  
  
  /**
   * Override to close all resources on program exit. By default this only invoked
   * on exception handling.
   */
  protected void close() {
    
  }

}
