#include <stddef.h>

#include "tslite.h"

#define ARRAY_TYPE_NULL 0
#define ARRAY_TYPE_INTEGER 1
#define ARRAY_TYPE_INTEGER_NEG 2
#define ARRAY_TYPE_FLOAT 3
#define ARRAY_TYPE_ZERO 4
#define ARRAY_TYPE_ONE 5
#define ARRAY_TYPE_BLOB 6
#define ARRAY_TYPE_TEXT 7

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

typedef union {
  double f;
  sqlite3_uint64 d;
} double_rep;

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

typedef struct {
  const void *arr;
  int len;
  int cap;
} array_buffer;

static int array_buffer_grow(array_buffer *buf, int n) {
  if (!buf->cap) {
    const void *z = sqlite3_malloc(n);
    if (!z) {
      return SQLITE_NOMEM;
    }
    buf->arr = z;
    buf->cap = n;
    return SQLITE_OK;
  }
  if (buf->cap - buf->len < n) {
    int nextCap = buf->cap * 2;
    while (buf->cap - buf->len < n) {
      nextCap *= 2;
    }
    const void *z = sqlite3_realloc(buf->arr, nextCap);
    if (!z) {
      return SQLITE_NOMEM;
    }
    buf->arr = z;
    buf->cap = nextCap;
  }
  return SQLITE_OK;
}

static int array_buffer_append(array_buffer *buf, const void *z, int n) {
  int res = array_buffer_grow(buf, n);
  if (!res) {
    return res;
  }
  memcopy(&buf->arr[buf->len], z, n);
}

static int array_buffer_append_byte(array_buffer *buf, char v) {
  int res = array_buffer_grow(buf, 1);
  if (!res) {
    return res;
  }
  (&buf->arr)[buf->len] = v;
  buf->len++;
  return SQLITE_OK;
}

static int array_buffer_append_uint64(array_buffer *buf, sqlite3_uint64 v) {
  int res = array_buffer_grow(buf, 8);
  if (!res) {
    return res;
  }
  put_u64(&buf->arr[buf->len], 8);
  buf->len += 8;
  return SQLITE_OK;
}

static int *array_buffer_append_value(array_buffer *buf, sqlite3_value *item) {
  int res;

  int type = sqlite3_value_type(item);
  switch (type) {
    case SQLITE_NULL:
      res = array_buffer_append_byte(buf, ARRAY_TYPE_NULL);
      break;

    case SQLITE_INTEGER:
      sqlite3_int64 v = sqlite3_value_int64(item);
      if (v == 0) {
        res = array_buffer_append_byte(buf, ARRAY_TYPE_ZERO);
      } else if (v == 1) {
        res = array_buffer_append_byte(buf, ARRAY_TYPE_ONE);
      } else {
        if (v < 0) {
          res = array_buffer_append_byte(buf, ARRAY_TYPE_INTEGER_NEG);
          v = -v;
        } else {
          res = array_buffer_append_byte(buf, ARRAY_TYPE_INTEGER);
        }
        if (!res) {
          return res;
        }
        res = array_buffer_grow(buf, 9);
        if (!res) {
          return res;
        }
        int size = put_varint64(&buf->arr[buf->len], v);
        buf->len += size;
      }
      break;

    case SQLITE_FLOAT:
      double_rep vF;
      vF.f = sqlite3_value_double(item);
      res = array_buffer_grow(buf, 9);
      if (!res) {
        return res;
      }
      res = array_buffer_append_byte(buf, ARRAY_TYPE_FLOAT);
      if (!res) {
        return res;
      }
      res = array_buffer_append_uint64(buf, vF.d);
      break;

    default:
      res = SQLITE_ERROR;
      break;
  }

  return res;
}

static void array_func(sqlite3_context *context, int argc,
                       sqlite3_value **argv) {
  if (argc < 1) {
    sqlite3_result_blob(context, NULL, 0, NULL);
    return;
  }

  int res;
  array_buffer buf = {NULL, 0, 0};
  for (int i = 0; i < argc; i++) {
    res = array_buffer_append_value(&buf, argv[i]);
    if (!res) {
      sqlite3_result_error(context, "serialization error", -1);
      return;
    }
  }

  sqlite3_result_blob(context, buf.arr, buf.len, NULL);
}
