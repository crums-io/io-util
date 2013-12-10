/*
 * Copyright 2013 Babak Farhang 
 */
package com.gnahraf.io.store.table.iter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * A <tt>RowIterator</tt> with a precedence tag. This comes into play when
 * composing a <tt>RowIterator</tt> from multiple other ones: the higher
 * precedence iterators <em>override</em> (overwrite) the lower precedence
 * ones if their next rows are equal (as defined by the comparator).
 * 
 * @author Babak
 */
public class PrecedenceRowIterator extends BufferedRowIterator
    implements Comparable<PrecedenceRowIterator> {
  
  private final int precedence;
  private final Comparator<ByteBuffer> rowOrder;

  public PrecedenceRowIterator(RowIterator impl, int precedence, Comparator<ByteBuffer> rowOrder)
      throws IOException {
    super(impl);
    this.precedence = precedence;
    this.rowOrder = rowOrder;
  }
  
  
  public final int getPrecedence() {
    return precedence;
  }
  
  
  public final Comparator<ByteBuffer> getRowOrder() {
    return rowOrder;
  }


  @Override
  public int compareTo(PrecedenceRowIterator other) {
    if (hasNext()) {
      if (other.hasNext()) {
        int comp = getDirection().effectiveComp(rowOrder.compare(peek(), other.peek()));
        return comp == 0 ?  other.precedence - this.precedence : comp;
      } else
        return -1;
    } else {
      return other.hasNext() ? 1 : 0;
    }
  }

}
