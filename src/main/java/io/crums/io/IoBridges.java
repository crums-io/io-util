/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.io;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.CharBuffer;
import java.util.Objects;

/**
 * Conversion utility methods for Java's mishmash of I/O streams.
 * Adaptor classes gathered here. Mostly one-offs accessed via
 * pseudo constructors. Adding these as the need arises.
 */
public class IoBridges {
  
  // never invoked
  private IoBridges() {  }
  
  
  /**
   * Returns a {@code PrintStream} as a {@code Writer}.
   */
  public static Writer toWriter(PrintStream out) {
    Objects.requireNonNull(out);
    // this shouldn't matter, but to be safe..
    boolean user = out != System.out && out != System.err;
    return new PrintStreamWriter(out, user);
  }
  
  
  private static class PrintStreamWriter extends Writer {
    
    private final PrintStream out;
    private final boolean close;
    
    PrintStreamWriter(PrintStream out, boolean close) {
      this.out = out;
      this.close = close;
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
      out.append(CharBuffer.wrap(cbuf, off, len));
      
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }

    @Override
    public void close() throws IOException {
      if (close)
        out.close();
    }

    @Override
    public void write(int c) throws IOException {
      out.append((char) c);
    }

    @Override
    public void write(String str) throws IOException {
      out.append(str);
    }

    @Override
    public void write(String str, int off, int len) throws IOException {
      out.append(str, off, off + len);
    }

    @Override
    public Writer append(CharSequence csq) throws IOException {
      out.append(csq);
      return this;
    }

    @Override
    public Writer append(CharSequence csq, int start, int end) throws IOException {
      out.append(csq, start, end);
      return this;
    }

    @Override
    public Writer append(char c) throws IOException {
      out.append(c);
      return this;
    }
    
  }

}
