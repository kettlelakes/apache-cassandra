/**
 * Java port of 32 and 64 bit MurmurHash2.
 * See http://sites.google.com/site/murmurhash/
 */
package org.apache.cassandra.utils;

public class LongMurmurHash {
  private static final long m64 = 0xc6a4a7935bd1e995L;
  private static final int r64 = 47;

  private static final int m32 = 0x5bd1e995;
  private static final int r32 = 24;

  public static long longHash(byte[] key, long seed) {
    int len = key.length;
    long h64 = (seed & 0xffffffffL) ^ (m64 * len);

    int lenLongs = len >> 3;

    for (int i = 0; i < lenLongs; ++i) {
      int offset = i << 3;
      long k64 = key[offset + 7];
      for (int j = 6; j >= 0; --j) {
        k64 <<= 8;
        k64 |= key[offset + j];
      }
      k64 *= m64;
      k64 ^= k64 >>> r64;
      k64 *= m64;

      h64 ^= k64;
      h64 *= m64;
    }

    int rem = len & 0x7;

    switch(rem) {
      case 0:
        break;
      case 7: h64 ^= (long)key[len - rem + 6] << 48;
      case 6: h64 ^= (long)key[len - rem + 5] << 40;
      case 5: h64 ^= (long)key[len - rem + 4] << 32;
      case 4: h64 ^= (long)key[len - rem + 3] << 24;
      case 3: h64 ^= (long)key[len - rem + 2] << 16;
      case 2: h64 ^= (long)key[len - rem + 1] << 8;
      case 1: h64 ^= (long)key[len - rem];
        h64 *= m64;
    }


    h64 ^= h64 >>> r64;
    h64 *= m64;
    h64 ^= h64 >>> r64;

    return h64;
  }

  public static int intHash(byte[] key, int seed) {
    int len = key.length;
    int h32 = seed ^ len;

    int lenInts = len >> 2;

    for (int i = 0; i < lenInts; ++i) {
      int offset = i << 2;
      int k32 = key[offset + 3];
      for (int j = 2; j >= 0; --j) {
        k32 <<= 8;
        k32 |= key[offset + j];
      }
      k32 *= m32;
      k32 ^= k32 >>> r32;
      k32 *= m32;

      h32 *= m32;
      h32 ^= k32;
    }

    int rem = len & 0x3;

    switch (rem) {
      case 0:
        break;
      case 3: h32 ^= (int)key[len - rem + 2] << 16;
      case 2: h32 ^= (int)key[len - rem + 1] << 8;
      case 1: h32 ^= (int)key[len - rem];
        h32 *= m32;
    }

    h32 ^= h32 >>> 13;
    h32 *= m32;
    h32 ^= h32 >>> 15;

    return h32;
  }
}
