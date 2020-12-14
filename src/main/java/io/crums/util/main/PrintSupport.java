/*
 * Copyright 2017 Babak Farhang
 */
package io.crums.util.main;


import java.io.PrintStream;

/**
 *
 */
public class PrintSupport {
  
  
  public static String fenceCharsWithSpace(String string, int spaces) {
    
    if (spaces < 0 || spaces > 160) 
      throw new IllegalArgumentException("spaces: " + spaces);
    
    StringBuilder fence = new StringBuilder(spaces);
    for (int count = spaces; count-- > 0; )
      fence.append(' ');
    
    return fenceChars(string, fence.toString());
  }
  
  
  
  public static String fenceChars(String string, String fence) {
    if (fence.isEmpty() || string.length() <= 1)
      return string;
    StringBuilder fenced =  new StringBuilder((string.length() - 1) * fence.length());
    fenced.append(string.charAt(0));
    for (int index = 1; index < string.length(); ++index)
      fenced.append(fence).append(string.charAt(index));
    return fenced.toString();
  }
  
  
  
  
  
  
  
  
  
  
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
  
  /**
   * 
   * @param restOfLine
   * @param center index of center on the console, typically near 40
   */
  public void printCentered(String restOfLine, int center) {
    int len = restOfLine.length();
    int spacePadding = center - getCharsWrittenToLine() - (len / 2);
    printSpaces(spacePadding);
    print(restOfLine);
  }
  
  /**
   * 
   * @param restOfLine
   * @param center index of center on the console, typically near 40
   * @param spread number of spaces between adjacent characters
   */
  public void printCenteredSpread(String restOfLine, int center, int spread) {
    restOfLine = fenceCharsWithSpace(restOfLine, spread);
    printCentered(restOfLine, center);
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
