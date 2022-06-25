#ifndef TSLITE_ARRAY_BUFFER_H
#define TSLITE_ARRAY_BUFFER_H

#include <string.h>

#include "tslite.h"

// Write a 32-bit unsigned integer as 4 big-endian bytes.
// Source from lsmVarintWrite32 in sqlite3/ext/lsm1/lsm_varint.c
static void put_u32(unsigned char *z, unsigned int y) {
  z[0] = (unsigned char)(y >> 24);
  z[1] = (unsigned char)(y >> 16);
  z[2] = (unsigned char)(y >> 8);
  z[3] = (unsigned char)(y);
}

// Write a 64-bit unsigned integer as 8 big-endian bytes.
static void put_u64(unsigned char *z, sqlite3_uint64 y) {
  z[0] = (unsigned char)(y >> 56);
  z[1] = (unsigned char)(y >> 48);
  z[2] = (unsigned char)(y >> 40);
  z[3] = (unsigned char)(y >> 32);
  z[4] = (unsigned char)(y >> 24);
  z[5] = (unsigned char)(y >> 16);
  z[6] = (unsigned char)(y >> 8);
  z[7] = (unsigned char)(y);
}

static sqlite3_uint64 get_u64(unsigned char *z) {
  return (((sqlite3_uint64)z[0]) | ((sqlite3_uint64)z[1]) << 8 |
          ((sqlite3_uint64)z[2]) << 16 | ((sqlite3_uint64)z[3]) << 24 |
          ((sqlite3_uint64)z[4]) << 32 | ((sqlite3_uint64)z[5]) << 40 |
          ((sqlite3_uint64)z[6]) << 48 | ((sqlite3_uint64)z[7]) << 56);
}

// Write a 64-bit variable-length integer to memory starting at p[0].
// The length of data write will be between 1 and 9 bytes.  The number
// of bytes written is returned.
//
// A variable-length integer consists of the lower 7 bits of each byte
// for all bytes that have the 8th bit set and one byte with the 8th
// bit clear.  Except, if we get to the 9th byte, it stores the full
// 8 bits and is the last byte.
//
// Source from putVarint64 in sqlite3/src/util.c.
static int put_varint64(unsigned char *p, sqlite3_uint64 v) {
  if (v <= 0x7f) {
    p[0] = v & 0x7f;
    return 1;
  }
  if (v <= 0x3fff) {
    p[0] = ((v >> 7) & 0x7f) | 0x80;
    p[1] = v & 0x7f;
    return 2;
  }

  int i, j, n;
  unsigned char buf[10];
  if (v & (((sqlite3_uint64)0xff000000) << 32)) {
    p[8] = (unsigned char)v;
    v >>= 8;
    for (i = 7; i >= 0; i--) {
      p[i] = (unsigned char)((v & 0x7f) | 0x80);
      v >>= 7;
    }
    return 9;
  }
  n = 0;
  do {
    buf[n++] = (unsigned char)((v & 0x7f) | 0x80);
    v >>= 7;
  } while (v != 0);
  buf[0] &= 0x7f;
  for (i = 0, j = n - 1; j >= 0; j--, i++) {
    p[i] = buf[j];
  }
  return n;
}

// Read a 64-bit variable-length integer from memory starting at p[0].
// Return the number of bytes read.  The value is stored in *v.
//
// Source from sqlite3GetVarint in sqlite3/src/util.c.
static unsigned char get_varint(unsigned char *p, sqlite3_uint64 *v) {
#define SLOT_2_0 0x001fc07f
#define SLOT_4_2_0 0xf01fc07f

  unsigned int a, b, s;

  if (((signed char *)p)[0] >= 0) {
    *v = *p;
    return 1;
  }
  if (((signed char *)p)[1] >= 0) {
    *v = ((unsigned int)(p[0] & 0x7f) << 7) | p[1];
    return 2;
  }

  a = ((unsigned int)p[0]) << 14;
  b = p[1];
  p += 2;
  a |= *p;
  if (!(a & 0x80)) {
    a &= SLOT_2_0;
    b &= 0x7f;
    b = b << 7;
    a |= b;
    *v = a;
    return 3;
  }

  a &= SLOT_2_0;
  p++;
  b = b << 14;
  b |= *p;
  if (!(b & 0x80)) {
    b &= SLOT_2_0;
    a = a << 7;
    a |= b;
    *v = a;
    return 4;
  }

  b &= SLOT_2_0;
  s = a;

  p++;
  a = a << 14;
  a |= *p;
  if (!(a & 0x80)) {
    b = b << 7;
    a |= b;
    s = s >> 18;
    *v = ((sqlite3_uint64)s) << 32 | a;
    return 5;
  }

  s = s << 7;
  s |= b;

  p++;
  b = b << 14;
  b |= *p;
  if (!(b & 0x80)) {
    a &= SLOT_2_0;
    a = a << 7;
    a |= b;
    s = s >> 18;
    *v = ((sqlite3_uint64)s) << 32 | a;
    return 6;
  }

  p++;
  a = a << 14;
  a |= *p;
  if (!(a & 0x80)) {
    a &= SLOT_4_2_0;
    b &= SLOT_2_0;
    b = b << 7;
    a |= b;
    s = s >> 11;
    *v = ((sqlite3_uint64)s) << 32 | a;
    return 7;
  }

  a &= SLOT_2_0;
  p++;
  b = b << 14;
  b |= *p;
  if (!(b & 0x80)) {
    b &= SLOT_4_2_0;
    a = a << 7;
    a |= b;
    s = s >> 4;
    *v = ((sqlite3_uint64)s) << 32 | a;
    return 8;
  }

  p++;
  a = a << 15;
  a |= *p;

  b &= SLOT_2_0;
  b = b << 8;
  a |= b;

  s = s << 4;
  b = p[-4];
  b &= 0x7f;
  b = b >> 3;
  s |= b;

  *v = ((sqlite3_uint64)s) << 32 | a;

  return 9;
}

// Double representation.
typedef union {
  double f;
  sqlite3_uint64 d;
} double_rep;

typedef struct {
  unsigned char *buf;
  int len, cap;
} array_buffer;

static int array_buffer_grow(array_buffer *buf, int n) {
  if (!buf->cap) {
    unsigned char *z = (unsigned char *)sqlite3_malloc(n);
    if (!z) {
      return SQLITE_NOMEM;
    }
    buf->buf = z;
    buf->cap = n;
    buf->len = 0;
    return SQLITE_OK;
  }
  if (buf->cap - buf->len < n) {
    int nextCap = buf->cap * 2;
    while (nextCap - buf->len < n) {
      nextCap *= 2;
    }
    unsigned char *z = (unsigned char *)sqlite3_realloc(buf->buf, nextCap);
    if (!z) {
      return SQLITE_NOMEM;
    }
    buf->buf = z;
    buf->cap = nextCap;
  }
  return SQLITE_OK;
}

#define array_buffer_end(buf) &(buf->buf[buf->len])

static int array_buffer_append(array_buffer *buf, unsigned char *z, int n) {
  int res = array_buffer_grow(buf, n);
  if (res) {
    return res;
  }
  memcpy(array_buffer_end(buf), z, n);
  buf->len += n;
  return SQLITE_OK;
}

static int array_buffer_append_byte(array_buffer *buf, unsigned char v) {
  int res = array_buffer_grow(buf, 1);
  if (res) {
    return res;
  }
  buf->buf[buf->len] = v;
  buf->len++;
  return SQLITE_OK;
}

static int array_buffer_append_uint64(array_buffer *buf, sqlite3_uint64 v) {
  int res = array_buffer_grow(buf, 8);
  if (res) {
    return res;
  }
  put_u64(array_buffer_end(buf), v);
  buf->len += 8;
  return SQLITE_OK;
}

static int array_buffer_append_double(array_buffer *buf, double v) {
  int res = array_buffer_grow(buf, 8);
  if (res) {
    return res;
  }
  double_rep f64_value;
  f64_value.f = v;
  put_u64(array_buffer_end(buf), f64_value.d);
  buf->len += 8;
  return SQLITE_OK;
}

static int array_buffer_append_varint64(array_buffer *buf, sqlite3_uint64 v) {
  int res = array_buffer_grow(buf, 9);
  if (res) {
    return res;
  }
  int s = put_varint64(array_buffer_end(buf), v);
  buf->len += s;
  return SQLITE_OK;
}

#endif  // TSLITE_ARRAY_BUFFER_H
