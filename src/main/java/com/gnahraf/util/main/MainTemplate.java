/*
 * Copyright 2020 Babak Farhang
 */
package com.gnahraf.util.main;


import static com.gnahraf.util.main.Args.*;

import java.io.PrintStream;

import com.gnahraf.util.UnanonymousType;

/**
 * If many console programs start to look alike, well you try..
 * <p>
 * All the methods in this class are <tt>protected</tt> because they
 * do dangerous things--like exit the program. 
 * </p>
 * 
 * @see Args
 */
public abstract class MainTemplate extends UnanonymousType {
  
  public final static String DEBUG = "debug";
  public final static String DEBUG_CMD = DEBUG + "=true";
  
  protected final static String ERROR_TAG = "[ERROR] ";
  

  
  
  /**
   * Launch point handling help and errors.
   * 
   * @param args  the arguments passed in from static main
   * 
   * @see #mainImpl(String[])
   */
  protected void doMain(String[] args) {
    if (help(args))
      printHelpAndExit();
    
    try {
      
      mainImpl(args);
    
    } catch (IllegalArgumentException iax) {
      
      exitInputError(makeErrorMessage(iax));
    
    } catch (Exception e) {
      
      exitOnProgramError(e, args);
    }
  }
  
  /**
   * Exits the program on error (exit code 2).
   * 
   * @param e     the exception indicating the error condition
   * @param args  the arguments passed in from static main
   */
  protected void exitOnProgramError(Exception e, String[] args) {
    printError(makeErrorMessage(e));
    if (debugEnabled(args))
      e.printStackTrace(System.err);
    System.exit(2);
  }
  
  private String makeErrorMessage(Exception e) {
    String message = e.getMessage();
    return e.getClass().getSimpleName() + (message == null ? "" : (" "  + message));
  }
  
  protected boolean debugEnabled(String[] args) {
    return contains(args, DEBUG_CMD);
  }
  
  /**
   * The meat of the program, invoked by {@linkplain #doMain(String[])}.
   * 
   * @param args                        arguments passed in from static main
   * @throws IllegalArgumentException   indicates a user input error not detected early on
   *                                    (as with {@linkplain #getRequiredParam(String[], String)} for e.g.)
   * @throws Exception                  indicates a program failure
   */
  protected abstract void mainImpl(String[] args) throws IllegalArgumentException, Exception;
  
  /**
   * Prints the given message on <tt>System.err</tt>, followed by an empty line.
   */
  protected void printError(String message) {
    System.err.println(ERROR_TAG + message);
    System.err.println();
  }
  
  /**
   * Exits the program after printing the given error <tt>message</tt>,
   * followed by the {@linkplain #printUsage(PrintStream)}. Exit status
   * code 1.
   */
  protected void exitInputError(String message) {
    printError(message);
    printUsage(System.err);
    System.err.println("Input '-help' for description details");
    System.err.println();
    System.exit(1);
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
    System.exit(0);
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

}
