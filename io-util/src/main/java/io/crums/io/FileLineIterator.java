/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;
import java.util.Objects;

import io.crums.util.CloseableIterator;

/**
 * Iterates over the lines in a text file. Maybe there's a cooler functional
 * way to do this, say by streaming, but until I learn how, this will do.
 */
public class FileLineIterator implements CloseableIterator<String> {

  private final File textFile;
  private final BufferedReader reader;
  
  private String nextLine;
  
  private long lines;

  /**
   * 
   */
  public FileLineIterator(File textFile) throws UncheckedIOException {
    this.textFile = Objects.requireNonNull(textFile, "null file");
    try {
      this.reader = new BufferedReader(new FileReader(textFile));
    } catch (IOException iox) {
      throw new UncheckedIOException("on creating instance with " + textFile, iox);
    }
    prepareNext();
  }
  
  
  private void prepareNext() {
    try {
      nextLine = reader.readLine();
      ++lines;
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
  //  I N T E R F A C E   M E T H O D S
  
  

  @Override
  public boolean hasNext() {
    return nextLine != null;
  }

  /**
   * <p>Returns the next line.</p>
   * {@inheritDoc}
   * 
   * @return non-null
   */
  @Override
  public String next() throws NoSuchElementException {
    if (nextLine == null)
      throw new NoSuchElementException();
    String out = nextLine;
    prepareNext();
    return out;
  }

  /**
   * Closes the underlying file stream.
   */
  @Override
  public void close() {
    try {
      reader.close();
    } catch (IOException iox) {
      throw new UncheckedIOException("I/O error encountered on closing " + this + ": " + iox, iox);
    }
  }
  

  //  E X T R A   G O O D I E S
  
  /**
   * Returns the number of lines read thus far.
   */
  public long linesRead() {
    return lines - 1;
  }
  
  
  /**
   * Returns the text file.
   */
  public File getFile() {
    return textFile;
  }
  
  /**
   * Advances the state of the instance so that the {@linkplain #next() next} line read will
   * at the given number. Returns true, if succeeded. Reasons for failure (return false) include the file
   * having too few lines, or if the iterator is already beyond the given line number.
   * 
   * @param lineNumber zero-based line number
   * @return {@code true} <b>iff</b> on return {@linkplain #linesRead()}{@code  == lineNumber }
   *         <em>and</em> {@linkplain #hasNext()} is {@code true}
   */
  public boolean advanceToLine(long lineNumber) {
    if (lineNumber < 0)
      throw new IllegalArgumentException("negative lineNumber " + lineNumber);
    
    while (linesRead() < lineNumber && hasNext())
      next();
    
    return linesRead() == lineNumber && hasNext();
  }
  
  
  public String toString() {
    return FileLineIterator.class.getSimpleName() + "[file=" + textFile + "]";
  }

}
