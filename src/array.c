#include "array.h"

#include "array_buffer.h"
#include "array_each.c"

static int array_buffer_append_value(array_buffer *buf, sqlite3_value *item) {
  int res;

  int type = sqlite3_value_type(item);
  int s;
  unsigned char *z;
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
        if (res) {
          return res;
        }
        res = array_buffer_append_varint64(buf, v);
      }
      break;

    case SQLITE_FLOAT:
      double vDouble = sqlite3_value_double(item);
      res = array_buffer_grow(buf, 9);
      if (res) {
        return res;
      }
      array_buffer_append_byte(buf, ARRAY_TYPE_FLOAT);
      array_buffer_append_double(buf, vDouble);
      break;

    case SQLITE_BLOB:
      s = sqlite3_value_bytes(item);
      z = (unsigned char *)sqlite3_value_blob(item);
      res = array_buffer_grow(buf, 10);
      if (res) {
        return res;
      }
      array_buffer_append_byte(buf, ARRAY_TYPE_BLOB);
      array_buffer_append_varint64(buf, (sqlite3_uint64)s);
      res = array_buffer_append(buf, z, s);
      break;

    case SQLITE_TEXT:
      s = sqlite3_value_bytes(item);
      z = (unsigned char *)sqlite3_value_text(item);
      res = array_buffer_grow(buf, 10);
      if (res) {
        return res;
      }
      array_buffer_append_byte(buf, ARRAY_TYPE_TEXT);
      array_buffer_append_varint64(buf, (sqlite3_uint64)s);
      res = array_buffer_append(buf, z, s);
      break;

    default:
      res = SQLITE_ERROR;
      break;
  }

  return res;
}

static int array_value_advance(unsigned char *z, int n) {
  if (!z || n < 1) {
    return -1;
  }

  int x;
  switch (*z) {
    case ARRAY_TYPE_NULL:
    case ARRAY_TYPE_ONE:
    case ARRAY_TYPE_ZERO:
      return 1;

    case ARRAY_TYPE_INTEGER:
    case ARRAY_TYPE_INTEGER_NEG:
      for (x = 1; x < n; x++) {
        if (!(z[x] & 0x80)) {
          if (x > 1 + 9) {  // 1 byte type identifier + max varint (9 bytes).
            return -1;
          }
          return x + 1;
        }
      }
      return -1;

    case ARRAY_TYPE_FLOAT:
      if (n < 9) {
        return -1;
      }
      return 9;

    case ARRAY_TYPE_BLOB:
    case ARRAY_TYPE_TEXT:
      if (n < 2) {
        return -1;
      }
      sqlite3_uint64 v;
      x = get_varint(&z[1], &v);
      x = 1 + x + v;
      if (n < x) {
        return -1;
      }
      return x;
  }

  return -1;
}

static const char *array_value_type(unsigned char v) {
  switch (v) {
    case ARRAY_TYPE_NULL:
      return "null";

    case ARRAY_TYPE_ZERO:
    case ARRAY_TYPE_ONE:
    case ARRAY_TYPE_INTEGER:
    case ARRAY_TYPE_INTEGER_NEG:
      return "integer";

    case ARRAY_TYPE_FLOAT:
      return "real";

    case ARRAY_TYPE_TEXT:
      return "text";

    case ARRAY_TYPE_BLOB:
      return "blob";
  }

  return NULL;
}

static int array_value_decode(sqlite3_context *context, unsigned char *z,
                              int n) {
  if (!z || n < 1) {
    return -1;
  }

  int x;
  double_rep value;
  switch (*z) {
    case ARRAY_TYPE_NULL:
      sqlite3_result_null(context);
      return 1;

    case ARRAY_TYPE_ONE:
      sqlite3_result_int(context, 1);
      return 1;

    case ARRAY_TYPE_ZERO:
      sqlite3_result_int(context, 0);
      return 1;

    case ARRAY_TYPE_INTEGER:
    case ARRAY_TYPE_INTEGER_NEG:
      if (n < 2) {
        return -1;
      }
      sqlite3_int64 valueInt;
      x = 1 + get_varint(&z[1], &value.d);
      if (n < x) {
        return -1;
      }
      if (*z == ARRAY_TYPE_INTEGER_NEG) {
        valueInt = -value.d;
      } else {
        valueInt = value.d;
      }
      sqlite3_result_int64(context, valueInt);
      return x;

    case ARRAY_TYPE_FLOAT:
      if (n < 9) {
        return -1;
      }
      value.d = get_u64(&z[1]);
      sqlite3_result_double(context, value.f);
      return 9;

    case ARRAY_TYPE_BLOB:
    case ARRAY_TYPE_TEXT:
      if (n < 2) {
        return -1;
      }
      x = get_varint(&z[1], &value.d);
      unsigned char *p = &z[1 + x];
      x = 1 + x + value.d;
      if (n < x) {
        return -1;
      }
      if (*z == ARRAY_TYPE_TEXT) {
        sqlite3_result_text(context, (const char *)p, value.d,
                            SQLITE_TRANSIENT);
      } else {
        sqlite3_result_blob(context, (const char *)p, value.d,
                            SQLITE_TRANSIENT);
      }
      return x;
  }

  return -1;
}

void array_func(sqlite3_context *context, int argc, sqlite3_value **argv) {
  if (argc < 1) {
    sqlite3_result_blob(context, NULL, 0, NULL);
    return;
  }

  int res;
  array_buffer buf = {NULL, 0, 0};
  for (int i = 0; i < argc; i++) {
    res = array_buffer_append_value(&buf, argv[i]);
    if (res) {
      sqlite3_result_error(context, "serialization error", -1);
      return;
    }
  }

  sqlite3_result_blob(context, buf.buf, buf.len, SQLITE_TRANSIENT);
  sqlite3_free(buf.buf);
}

void array_length_func(sqlite3_context *context, int argc,
                       sqlite3_value **argv) {
  UNUSED(argc);

  int s = sqlite3_value_bytes(argv[0]);
  const unsigned char *z = sqlite3_value_blob(argv[0]);
  if (!z) {
    sqlite3_result_error_nomem(context);
    return;
  }

  int n = 0;
  while (s > 0) {
    int delta = array_value_advance((unsigned char *)z, s);
    if (delta == -1) {
      sqlite3_result_error(context, "malformed array", -1);
      return;
    }
    n++;
    z += delta;
    s -= delta;
  }

  sqlite3_result_int(context, n);
  return;
}

void array_append_func(sqlite3_context *context, int argc,
                       sqlite3_value **argv) {
  if (argc < 1) {
    sqlite3_result_error(context, "too few arguments to function array_append",
                         -1);
    return;
  }

  array_buffer buf = {NULL, 0, 0};

  int s = sqlite3_value_bytes(argv[0]);
  const unsigned char *z = (unsigned char *)sqlite3_value_blob(argv[0]);
  if (!z) {
    sqlite3_result_error_nomem(context);
    return;
  }

  int res;
  if (z) {
    res = array_buffer_append(&buf, (unsigned char *)z, s);
    if (res) {
      sqlite3_result_error(context, "array appendation error", -1);
      return;
    }
  }

  for (int i = 1; i < argc; i++) {
    res = array_buffer_append_value(&buf, argv[i]);
    if (res) {
      sqlite3_result_error(context, "serialization error", -1);
      sqlite3_free(buf.buf);
      return;
    }
  }

  sqlite3_result_blob(context, buf.buf, buf.len, SQLITE_TRANSIENT);
  sqlite3_free(buf.buf);
}

void array_at_func(sqlite3_context *context, int argc, sqlite3_value **argv) {
  UNUSED(argc);

  int s = sqlite3_value_bytes(argv[0]);
  const unsigned char *z = sqlite3_value_blob(argv[0]);
  if (!z) {
    sqlite3_result_error_nomem(context);
    return;
  }
  int i = sqlite3_value_int(argv[1]);
  if (i < 0) {
    goto err_oob;
  }

  int delta;
  for (int j = 0; j < i; j++) {
    if (s <= 0) {
      goto err_oob;
    }
    delta = array_value_advance((unsigned char *)z, s);
    if (delta == -1) {
      goto err_malformed;
    }
    z += delta;
    s -= delta;
  }

  delta = array_value_decode(context, (unsigned char *)z, s);
  if (delta == -1) {
    goto err_malformed;
  }
  return;

err_oob:
  sqlite3_result_error(context, "index out of bounds", -1);
  return;

err_malformed:
  sqlite3_result_error(context, "malformed array", -1);
  return;
}

void array_agg_step_func(sqlite3_context *context, int argc,
                         sqlite3_value **argv) {
  if (argc < 1) {
    return;
  }

  array_buffer *buf = sqlite3_aggregate_context(context, sizeof(array_buffer));
  if (!buf) {
    sqlite3_result_error_nomem(context);
    return;
  }

  int rc;
  for (int i = 0; i < argc; i++) {
    rc = array_buffer_append_value(buf, argv[i]);
    if (!rc) {
      sqlite3_result_error_code(context, rc);
      return;
    }
  }
}

void array_agg_final_func(sqlite3_context *context) {
  array_buffer *buf = sqlite3_aggregate_context(context, sizeof(array_buffer));
  if (!buf) {
    sqlite3_result_error_nomem(context);
    return;
  }
  sqlite3_result_blob(context, buf->buf, buf->len, SQLITE_TRANSIENT);
  sqlite3_free(buf->buf);
}

void array_agg_value_func(sqlite3_context *context) {
  array_buffer *buf = sqlite3_aggregate_context(context, sizeof(array_buffer));
  if (!buf) {
    sqlite3_result_error_nomem(context);
    return;
  }
  sqlite3_result_blob(context, buf->buf, buf->len, SQLITE_TRANSIENT);
}
