/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.io;

import java.io.FilterOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
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
  
  
  /** Stateless, empty input stream. */
  public final static InputStream EMPTY_INPUT_STREAM = new InputStream() {
    @Override
    public int read() throws IOException { return -1; }
    
  };
  
  
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
  
  
  

  
  
  /**
   */
  /**
   * Returns an {@code InputStream} view of the given {@code RandomAccessFile}.
   * Bytes read advance the underlying stream's position by as many bytes.
   * The returned instance does not close the underlying stream.
   * 
   * @param raf     the underlying stream
   *                
   * @return        An {@code InputStream} view of the underlying stream starting from
   *                its current position
   */
  public static InputStream toInputStream(RandomAccessFile raf) {
    return new RandomAccessInputStream(raf, false);
  }

  
  
  /**
   * Returns an {@code InputStream} view of the given {@code RandomAccessFile}.
   * Bytes read advance the underlying stream's position by as many bytes.
   * 
   * @param raf     the underlying stream
   * @param closes  if {@code true} then the returned stream's {@code close()} method
   *                closes the underlying stream.
   *                
   * @return        An {@code InputStream} view of the underlying stream starting from
   *                its current position
   */
  public static InputStream toInputStream(RandomAccessFile raf, boolean closes) {
    return new RandomAccessInputStream(raf, closes);
  }
  
  

  /**
   * Returns an {@code InputStream} view of the given {@code RandomAccessFile}, starting
   * from the given offset and ending after the given number of bytes ({@code length})
   * are read. The returned view does not close the underlying stream.
   * 
   * @param raf     the underlying stream (note its state must not be modified before finishing
   *                use with the returned stream)
   * @param offset  the starting position in the file (0 &ge; {@code offset} &ge; {@code raf.length()})
   * @param length  the number of bytes to be read after the offset (&ge; 0)
   */
  public static InputStream sliceInputStream(
      RandomAccessFile raf, long offset, long length) throws IOException {
    return sliceInputStream(raf, offset, length, false);
  }
  
  
  /**
   * Returns an {@code InputStream} view of the given {@code RandomAccessFile}, starting
   * from the given offset and ending after the given number of bytes ({@code length})
   * are read.
   * 
   * @param raf     the underlying stream (note its state must not be modified before finishing
   *                use with the returned stream)
   * @param offset  the starting position in the file (0 &ge; {@code offset} &ge; {@code raf.length()})
   * @param length  the number of bytes to be read after the offset (&ge; 0)
   * @param closes  if {@code true} then the returned stream's {@code close()} method
   *                closes the underlying stream.
   */
  public static InputStream sliceInputStream(
      RandomAccessFile raf, long offset, long length,
      boolean closes) throws IOException {
    
    Objects.requireNonNull(raf, "null random access file");
    if (offset < 0)
      throw new IllegalArgumentException("negative offset: " + offset);
    if (length < 0)
      throw new IllegalArgumentException("negative length: " + length);
    
    long size = raf.length();

    if (offset + length > size)
      throw new IllegalArgumentException(
          "offset out-of-bounds (offset, length): (" + offset + ", " + length +
          "); file length " + size);
    
    if (length == 0)
      return EMPTY_INPUT_STREAM;
    
    raf.seek(offset);
    InputStream is = toInputStream(raf, closes);
    return new TruncatedInputStream(is, length, closes);
  }
  

  /**
   * Truncates the stream to the specified maximum length. The returned stream
   * does not close the underlying stream.
   * 
   * @param is      the underlying stream
   * @param length  <em>maximum</em> length of the stream (&ge; 0)
   */
  public static InputStream truncateInputStream(InputStream is, long length) {
    return truncateInputStream(is, length, false);
  }
  
  
  /**
   * Truncates the stream to the specified maximum length.
   * 
   * @param is      the underlying stream
   * @param length  <em>maximum</em> length of the stream (&ge; 0)
   * @param closes  if {@code true} then the returned stream's {@code close()} method
   *                closes the underlying stream.
   */
  public static InputStream truncateInputStream(InputStream is, long length, boolean closes) {
    return new TruncatedInputStream(is, length, closes);
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
  
  
  
  /** Boiler plate wrapper allowing an underlying stream to be closed, or not. */
  static abstract class OwnedInputStream extends InputStream {
    
    private final boolean owns;
    
    protected OwnedInputStream(boolean owns) { this.owns = owns; }
    
    /**
     * Conditionally closes the underlying base stream, if it's <em>owned</em>.
     * @see #ownsBase()
     */
    @Override
    public void close() throws IOException {
      if (owns)
        closeBase();
    }
    
    /** Determines whether the base is closed by this instance. */
    public boolean ownsBase() {
      return owns;
    }
    /** {@code close()} implementation when the underlying stream is owned. */
    protected abstract void closeBase() throws IOException;
    
  }
  
  
  
  /** {@code RandomAccessFile} to {@code InputStream} adaptor. */
  public static class RandomAccessInputStream extends OwnedInputStream {

    protected final RandomAccessFile raf;
    
    /** Creates an instance that does not own (close) the underlying stream. */
    public RandomAccessInputStream(RandomAccessFile raf) {
      this(raf, false);
    }
    
    /**
     * Full constructor.
     * 
     * @param raf   the underlying stream
     * @param owns  if {@code true}, then the underlying stream is actually closed on {@linkplain #close()}
     */
    public RandomAccessInputStream(RandomAccessFile raf, boolean owns) {
      super(owns);
      this.raf = Objects.requireNonNull(raf, "null raf");
    }
    
    

    @Override
    public int read() throws IOException {
      return raf.read();
    }

    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return raf.read(b, off, len);
    }
    
    protected void closeBase() throws IOException {
      raf.close();
    }
    
  }
  
  
  /** Truncates an {@code InputStream}. */
  public static class TruncatedInputStream extends OwnedInputStream {
    
    private final InputStream base;
    
    private long remaining;
    

    /**
     * Creates an instance that does not own (close) the underlying stream.
     * 
     * @param base    the underlying stream
     * @param length  the <em>maximum</em> length of the stream
     */
    public TruncatedInputStream(InputStream base, long length) {
      this(base, length, false);
    }
    
    /**
     * Full constructor.
     * 
     * @param base    the underlying stream
     * @param length  the <em>maximum</em> length of the stream
     * @param owns    if {@code true}, then the underlying stream is actually closed on {@linkplain #close()}
     */
    public TruncatedInputStream(InputStream base, long length, boolean owns) {
      super(owns);
      this.base = Objects.requireNonNull(base, "null base stream");
      if (length < 0)
        throw new IllegalArgumentException("negative length: " + length);
      this.remaining = length;
    }
    


    @Override
    public int read() throws IOException {
      if (remaining <= 0) {
        assert remaining == 0;
        return -1;
      }
      int b = base.read();
      if (b == -1)
        remaining = 0;
      else
        --remaining;
      return b;
    }
    

    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (remaining <= 0) {
        assert remaining == 0;
        return -1;
      }
      if (len > remaining)
        len = (int) remaining;
      
      int bytesRead = base.read(b, off, len);
      if (bytesRead == -1)
        remaining = 0;
      else
        remaining -= bytesRead;
      return bytesRead;
    }
    
    
    
    @Override
    protected void closeBase() throws IOException {
      base.close();
    }
    
    
    
  }
  
  
  

}














