/*
 * Copyright 2017 Babak Farhang
 */
package io.crums.util.main;

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.List;

import io.crums.util.Lists;

/**
 * Utility for printing columnar stuff to the console.
 */
public class TablePrint extends PrintSupport {
  
  
  private final int[] columnWidths;
  
  private boolean formatNumber;
  
  @SuppressWarnings("serial")
  private NumberFormat numberFormat =
      new DecimalFormat("#,###.###") {

        @Override
        public StringBuffer format(double number, StringBuffer result, FieldPosition fieldPosition) {
          if (number >= 0)
            result.append(' ');
          return super.format(number, result, fieldPosition);
        }

        @Override
        public StringBuffer format(long number, StringBuffer result, FieldPosition fieldPosition) {
          if (number >= 0)
            result.append(' ');
          return super.format(number, result, fieldPosition);
        }
      };

  /**
   * 
   */
  public TablePrint(int... columnWidths) {
    this(System.out, columnWidths);
  }

  /**
   * 
   * @param out
   */
  public TablePrint(PrintStream out, int... columnWidths) {
    super(out);
    this.columnWidths = columnWidths.clone();
    
    for (int index = columnWidths.length; index-- > 0; )
      if (this.columnWidths[index] < 0)
        throw new IllegalArgumentException(Arrays.toString(columnWidths));
  }
  
  
  public void setNumberFormat(NumberFormat numberFormat) {
    if (numberFormat == null)
      throw new IllegalArgumentException("null");
    this.numberFormat = numberFormat;
    formatNumber = true;
  }
  
  
  
  public void setFormatNumber(boolean on) {
    this.formatNumber = on;
  }
  
  
  public boolean isNumberFormatted() {
    return formatNumber;
  }
  
  
  /**
   * Prints a table row. To be invoked at beginning of new line.
   */
  public void printRow(Object... cells) {
    if (cells.length > columnWidths.length)
      throw new IllegalArgumentException("too many args: " + Arrays.asList(cells));
    
    for (int index = 0, cursor = 0; index < cells.length; ++index) {
      Object cell = cells[index];
      String string;
      if (formatNumber && cell instanceof Number)
        string = numberFormat.format(cell);
      else if (cell == null)
        string = "";
      else
        string = cell.toString();
      print(string);
      cursor += columnWidths[index];
      boolean padded = padToColumn(cursor);
      if (!padded)
        printSpaces(1);
    }
    println();
  }
  
  
  /**
   * Prints a table edge. To be invoked at beginning of new line.
   */
  public void printHorizontalTableEdge(char c) {
    printChar(c, getRowWidth());
    println();
  }
  
  
  /**
   * Returns the sum of the column widths.
   */
  public int getRowWidth() {
    int width = 0;
    for (int index = columnWidths.length; index-- > 0; )
      width += columnWidths[index];
    return width;
  }
  
  
  /**
   * Returns the column width at the given index.
   */
  public int getColWidth(int col) throws IndexOutOfBoundsException {
    return columnWidths[col];
  }
  
  
  /**
   * Returns the starting position of the given column.
   */
  public int getColStart(int col) throws IndexOutOfBoundsException {
    int tally = 0;
    for (int index = 0; index < col; ++index)
      tally += columnWidths[index];
    return tally;
  }
  
  
  public List<Integer> getColumnWidths() {
    return Lists.intList(columnWidths);
  }
  
  
  public int getCenter() {
    return getRowWidth() / 2;
  }
  
  
  
  public void printlnCentered(String restOfLine) {
    printCentered(restOfLine, getCenter());
    println();
  }
  
  
  public void printlnCenteredSpread(String restOfLine) {
    printlnCenteredSpread(restOfLine, 1);
  }
  
  
  public void printlnCenteredSpread(String restOfLine, int spread) {
    printCenteredSpread(restOfLine, getCenter(), spread);
    println();
  }

}
