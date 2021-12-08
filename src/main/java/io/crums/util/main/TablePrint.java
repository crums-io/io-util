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
  
  
  private String colSep = "";
  
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

  
  public TablePrint(List<Integer> columnWidths) {
    this(System.out, columnWidths);
  }
  
  
  public TablePrint(PrintStream out, List<Integer> columnWidths) {
    super(out);
    this.columnWidths = new int[columnWidths.size()];
    
    for (int index = columnWidths.size(); index-- > 0; )
      if ((this.columnWidths[index] = columnWidths.get(index)) < 0)
        throw new IllegalArgumentException(columnWidths.toString());
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
  
  
  public String getColSeparator() {
    return colSep;
  }


  public void setColSeparator(String colSep) {
    this.colSep = colSep == null ? "" : colSep;
  }


  /**
   * Prints a table row. To be invoked at beginning of new line.
   * 
   * @param cells 1 or more objects per cell. It's OK if there are more cells than declared columns.
   */
  public void printRow(Object... cells) {
    
    for (int index = 0; index < cells.length; ++index) {
      Object cell = cells[index];
      String string;
      if (formatNumber && cell instanceof Number)
        string = numberFormat.format(cell);
      else if (cell == null)
        string = "";
      else
        string = cell.toString();
      
      if (index > 0)
        print(colSep);
      
      print(string);

      final int charsWritten = getCharsWrittenToLine();
      
      if (index < columnWidths.length) {
        int happyPathIndex = getNetColumnEnd(index);
        if (charsWritten < happyPathIndex) {
          padToCharColumn(happyPathIndex);
          continue;
        }
      }
      
      padToCharColumn(charsWritten + (colSep.isEmpty() ? 1 : 0));
    }
    
    println();
  }
  
  
  /**
   * Returns the <em>net</em> declared column starting position. This includes
   * the {@linkplain #getIndentation() indentation}.
   * 
   * @param colIndex &ge; 0 and &le; {@linkplain #getColumnCount()}
   * 
   * @see #printRow(Object...)
   */
  public int getNetColumnStart(int colIndex) throws IndexOutOfBoundsException {
    int tally = getIndentation();
    for (int index = colIndex; index-- > 0; )
      tally += columnWidths[index];
    return tally;
  }
  
  
  /**
   * Returns the <em>net</em> declared column ending position. This includes
   * the {@linkplain #getIndentation() indentation}.
   * 
   * @param colIndex &ge; 0 and &lt; {@linkplain #getColumnCount()}
   * 
   * @return {@code getNetColumnStart(colIndex + 1)}
   * 
   * @see #printRow(Object...)
   */
  public int getNetColumnEnd(int colIndex) throws IndexOutOfBoundsException {
    return getNetColumnStart(colIndex + 1);
  }
  
  
  /**
   * Returns the number of declared columns.
   * 
   * @see #printRow(Object...)
   */
  public int getColumnCount() {
    return columnWidths.length;
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
