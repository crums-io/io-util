/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.io;

import java.io.FilterOutputStream;
import java.io.Flushable;
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
   * Returns the given {@code PrintStream} as a {@code Writer}.
   * 
   * @return a view of {@code out} as a {@linkplain Writer}
   */
  public static Writer toWriter(PrintStream out) {
    // this shouldn't matter, but to be safe..
    boolean user = out != System.out && out != System.err;
    return new AppendableWriterAdaptor(out, user);
  }
  
  

  
  /**
   * Returns the given {@linkplain Appendable} as a writer. The returned
   * writer's {@code close()} method does nothing.
   * 
   * @param out the appendable stream
   * 
   * @return a view of {@code out} as a {@linkplain Writer}
   */
  public static Writer toWriter(Appendable out) {
    return new AppendableWriterAdaptor(out, false);
  }
  
  
  
  /**
   * Returns the given {@linkplain Appendable} as a closing writer.
   * 
   * <h4>FIXME: </h4>
   * <p>For some reason, this &lt;T&gt; def doesn't work.. the compiler doesn't fuss if out is not Closeable.</p>
   * 
   * @param <T> type that is both {@linkplain Appendable} and {@linkplain AutoCloseable}
   * @param out the appendable stream
   * 
   * @return a view of {@code out} as a {@linkplain Writer}
   */
  public static <T extends Appendable, Closeable> Writer toCloseableWriter(T out) {
    return new AppendableWriterAdaptor(out, true);
  }
  
  
  private static class AppendableWriterAdaptor extends Writer {
    
    private final Appendable out;
    private final boolean close;
    
    AppendableWriterAdaptor(Appendable out, boolean close) {
      Objects.requireNonNull(out);
      this.out = out;
      this.close = close;
      if (close && !(out instanceof AutoCloseable))
        throw new IllegalArgumentException(out + " is not an instance of AutoCloseable");
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
      out.append(CharBuffer.wrap(cbuf, off, len));
      
    }

    @Override
    public void flush() throws IOException {
      if (out instanceof FilterOutputStream)
        ((FilterOutputStream) out).flush();
      else if (out instanceof Flushable) {
        ((Flushable) out).flush();
      }
    }

    @Override
    public void close() throws IOException {
      if (close) {
        try {
          ((AutoCloseable) out).close();
        } catch (IOException iox) {
          // rethrow
          throw iox;
        } catch (Exception x) {
          throw new IOException("on close: " + x.getMessage(), x);
        }
      }
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
