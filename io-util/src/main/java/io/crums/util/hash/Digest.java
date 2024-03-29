/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.hash;


import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Specifies a hashing method. This interface found its home in this
 * module, as its useful across a range of projects.
 */
public interface Digest {
  
  
  public static boolean compatible(Digest digest, MessageDigest md) {
    Objects.requireNonNull(digest, "null digest");
    Objects.requireNonNull(md, "null md");
    return
        md.getDigestLength() == digest.hashWidth() &&
        md.getAlgorithm().equals(digest.hashAlgo());
  }
  
  public static boolean equal(Digest a, Digest b) {
    Objects.requireNonNull(a, "null a");
    Objects.requireNonNull(b, "null b");
    return a.hashWidth() == b.hashWidth() && a.hashAlgo().equals(b.hashAlgo());
  }
  
  
  /**
   * Convenience method for returning the digest array as a read-only buffer.
   * 
   * @return {@code ByteBuffer.wrap(digest.digest()).asReadOnlyBuffer()}
   */
  public static ByteBuffer bufferDigest(MessageDigest digest) {
    Objects.requireNonNull(digest, "null digest");
    return ByteBuffer.wrap(digest.digest()).asReadOnlyBuffer();
  }
  
  
  /**
   * Returns the number of bytes used to form a hash.
   * 
   * @see MessageDigest#getDigestLength()
   */
  int hashWidth();
  
  
  /**
   * Returns the name of the hashing algorithm.
   * 
   * @see MessageDigest#getAlgorithm()
   */
  String hashAlgo();
  
  
  /**
   * Creates and returns a new <code>MessageDigest</code>. The
   * returned instance must match this specification.
   */
  default MessageDigest newDigest() {
    String algo = hashAlgo();
    try {
      
      MessageDigest digest = MessageDigest.getInstance(algo);
      assert digest.getDigestLength() == hashWidth();
      
      return digest;
      
    } catch (NoSuchAlgorithmException nsax) {
      throw new RuntimeException("on creating digest with algo " + algo, nsax);
    }
  }
  
  
  
  /**
   * Returns a read-only, buffer of zeroes with {@linkplain #hashWidth()}
   * remaining bytes. The default implementation is needlessly inefficient;
   * if this method is invoked often, consider overriding and returning a
   * duplicate of the same instance.
   */
  default ByteBuffer sentinelHash() {
    return ByteBuffer.allocate(hashWidth()).asReadOnlyBuffer();
  }
  
  

}
