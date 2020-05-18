/*
 * Copyright 2017 Babak Farhang
 */
package com.gnahraf.util.main;


import java.io.PrintStream;

/**
 *
 */
public class PrintSupport {
  
  protected final PrintStream out;
  
  private int indentation;

  private boolean lineBegun;
  private int charsWrittenToLine;
  
  public PrintSupport() {
    out = System.out;
  }
  
  public PrintSupport(PrintStream out) {
    this.out = out;
    if (out == null)
      throw new IllegalArgumentException("null out");
  }
  
  
  
  public int getIndentation() {
    return indentation;
  }
  
  public void setIndentation(int spaces) {
    if (spaces < 0)
      throw new IllegalArgumentException("spaces " + spaces);
    indentation = spaces;
  }
  
  
  /**
   * Returns the number of characters written to the line, ignoring
   * indentation.
   */
  public int getCharsWrittenToLine() {
    return charsWrittenToLine;
  }

  public void print(String lineSnippet) {
    printLineStart();
    out.print(lineSnippet);
    charsWrittenToLine += lineSnippet.length();
  }
  
  
  public void println(String restOfLine) {
    printLineStart();
    out.println(restOfLine);
    lineEnded();
  }
  
  
  private void lineEnded() {
    lineBegun = false;
    charsWrittenToLine = 0;
  }
  
  
  public void println() {
    out.println();
    lineEnded();
  }
  
  
  public boolean padToColumn(int column) {
    return padToColumn(column, ' ');
  }
  
  
  public boolean padToColumn(int column, char c) {
    int count = column - getCharsWrittenToLine();
    printChar(c, count);
    return count > 0;
  }
  
  
  protected final void printLineStart() {
    if (lineBegun)
      return;
    printIndentation();
    lineBegun = true;
  }
  
  
  protected void printIndentation() {
    printCharImpl(' ', getIndentation());
  }
  
  
  public void printSpaces(int count) {
    printChar(' ', count);
  }
  
  
  public void printChar(char c, int count) {
    printLineStart();
    printCharImpl(c, count);
    charsWrittenToLine += Math.max(0, count);
  }
  
  private void printCharImpl(char c, int count) {
    while (count-- > 0)
      out.print(c);
  }
  

}
