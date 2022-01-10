/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.io.readers;


import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;
import java.util.Objects;

import io.crums.util.Strings;


/**
 * Analogous to {@linkplain java.util.StringTokenizer} but for
 * {@code Reader}s instead of {@code String}s. <em>Designed for
 * single-thread access only.</em>
 */
public abstract class ReaderTokenizer implements Closeable {
  
  
  private final StringBuilder token = new StringBuilder(256);
  
  protected final Reader reader;
  
  private int charsRead;
  
  private int tokenCount;
  
  
  protected ReaderTokenizer(Reader reader) throws UncheckedIOException {
    this.reader = Objects.requireNonNull(reader, "null reader");
    readNextToken();
  }
  
  
  
  
  
  
  /**
   * Determines if there is a next token.
   *  
   * @return {@code true} iff {@linkplain #nextToken()} will succeed
   * on the next invocation
   */
  public boolean hasMoreTokens() {
    return token.length() != 0;
  }
  
  /**
   * Returns the next token.
   * 
   * @throws NoSuchElementException if there are no more tokens
   * @see #hasMoreTokens()
   */
  public String nextToken() throws NoSuchElementException {
    String out = token.toString();
    if (out.isEmpty())
      throw new NoSuchElementException();
    
    ++tokenCount;
    readNextToken();
    
    return out;
  }
  
  
  /**
   * Attempts to skip the next token and returns the result.
   */
  public boolean skipNextToken() {
    if (token.length() == 0)
      return false;
    
    ++tokenCount;
    readNextToken();
    return true;
  }
  
  /**
   * Returns the number of tokens dispensed thus far.
   */
  public int getTokenCount() {
    return tokenCount;
  }
  
  /**
   * Attempts to skip {@code count}-many tokens and returns the result.
   * 
   * @return {@code true} <b>iff</b> {@code count}-many tokens were skipped
   * before reaching the end of the stream
   */
  public boolean skipNextTokens(int count) {
    if (count < 0)
      throw new IllegalArgumentException("count " + count);
    while (count > 0 && skipNextToken()) {
      --count;
    }
    return count == 0;
  }
  
  /**
   * Skips the remaining tokens, if any.
   * 
   * @return {@code true} <b>iff</b> there were any remaining tokens to skip
   */
  public boolean skipRemaining() {
    if (!hasMoreTokens())
      return false;
    while (skipNextToken());
    return true;
  }
  
  private void readNextToken() throws UncheckedIOException {
    token.setLength(0);
    try {
      while (true) {
        int i = reader.read();
        if (i == -1)
          break;
        
        ++charsRead;
        char c = (char) i;
        if (isDelimiter(c)) {
          if (token.length() > 0)
            return;
        } else {
          token.append(c);
        }
      }
    
    } catch (IOException iox) {
      throw new UncheckedIOException("at char offset " + charsRead, iox);
    }
  }
  
  
  
  
  /**
   * Determines if the given character is a token delimiter.
   * 
   * @param c the char (&ge; 0)
   */
  protected abstract boolean isDelimiter(char c);
  
  /**
   * Returns the total number of character read thus far. Depending
   * on the character set / encoding, this may be fewer than the number
   * of bytes read.
   */
  public int getCharactersRead() {
    return charsRead;
  }
  
  
  /**
   * Closes the underlying reader.
   */
  public void close() throws IOException {
    reader.close();
  }
  
  
  
  
  /**
   * Returns an instance that tokenizes words delimited by whitespace.
   */
  public static ReaderTokenizer newWhitespaceInstance(Reader reader) throws UncheckedIOException {
    return new ReaderTokenizer(reader) {
      @Override
      protected boolean isDelimiter(char c) {
        return Strings.isWhitespace(c);
      }
    };
  }
  
  

  /**
   * Returns an instance that tokenizes lines.
   */
  public static ReaderTokenizer newLineInstance(Reader reader) throws UncheckedIOException {
    return new ReaderTokenizer(reader) {
      @Override
      protected boolean isDelimiter(char c) {
        switch (c) {
        case '\n':
        case '\r':
          return true;
        }
        return false;
      }
    };
  }
  

}





