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

// Write a varint into z[]. The buffer z[] must be at least 9 characters
// long to accommodate the largest possible varint. Return the number of
// bytes of z[] used.
// Source from lsmSqlite4PutVarint64 in sqlite3/ext/lsm1/lsm_varint.c
static int put_varint64(unsigned char *z, sqlite3_uint64 x) {
  unsigned int w, y;
  if (x <= 240) {
    z[0] = (unsigned char)x;
    return 1;
  }
  if (x <= 2287) {
    y = (unsigned int)(x - 240);
    z[0] = (unsigned char)(y / 256 + 241);
    z[1] = (unsigned char)(y % 256);
    return 2;
  }
  if (x <= 67823) {
    y = (unsigned int)(x - 2288);
    z[0] = 249;
    z[1] = (unsigned char)(y / 256);
    z[2] = (unsigned char)(y % 256);
    return 3;
  }
  y = (unsigned int)x;
  w = (unsigned int)(x >> 32);
  if (w == 0) {
    if (y <= 16777215) {
      z[0] = 250;
      z[1] = (unsigned char)(y >> 16);
      z[2] = (unsigned char)(y >> 8);
      z[3] = (unsigned char)(y);
      return 4;
    }
    z[0] = 251;
    put_u32(z + 1, y);
    return 5;
  }
  if (w <= 255) {
    z[0] = 252;
    z[1] = (unsigned char)w;
    put_u32(z + 2, y);
    return 6;
  }
  if (w <= 32767) {
    z[0] = 253;
    z[1] = (unsigned char)(w >> 8);
    z[2] = (unsigned char)w;
    put_u32(z + 3, y);
    return 7;
  }
  if (w <= 16777215) {
    z[0] = 254;
    z[1] = (unsigned char)(w >> 16);
    z[2] = (unsigned char)(w >> 8);
    z[3] = (unsigned char)w;
    put_u32(z + 4, y);
    return 8;
  }
  z[0] = 255;
  put_u32(z + 1, w);
  put_u32(z + 5, y);
  return 9;
}

// Read a varint. Put the value in *pVal and return the number of bytes.
// Source from dbdataGetVarint in sqlite3/ext/misc/dbdata.c.
static int get_varint(const unsigned char *z, sqlite3_int64 *pVal) {
  sqlite3_int64 v = 0;
  int i;
  for (i = 0; i < 8; i++) {
    v = (v << 7) + (z[i] & 0x7f);
    if ((z[i] & 0x80) == 0) {
      *pVal = v;
      return i + 1;
    }
  }
  v = (v << 8) + (z[i] & 0xff);
  *pVal = v;
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
