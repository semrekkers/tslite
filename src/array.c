#include "array.h"

#include "array_buffer.h"

#define ARRAY_TYPE_NULL 0
#define ARRAY_TYPE_INTEGER 1
#define ARRAY_TYPE_INTEGER_NEG 2
#define ARRAY_TYPE_FLOAT 3
#define ARRAY_TYPE_ZERO 4
#define ARRAY_TYPE_ONE 5
#define ARRAY_TYPE_BLOB 6
#define ARRAY_TYPE_TEXT 7

static int array_buffer_append_value(array_buffer *buf, sqlite3_value *item) {
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

    default:
      res = SQLITE_ERROR;
      break;
  }

  return res;
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
  for (int i = 0; i < s; i++) {
    switch (z[i]) {
      case ARRAY_TYPE_NULL:
      case ARRAY_TYPE_ZERO:
      case ARRAY_TYPE_ONE:
        n++;
        break;

      case ARRAY_TYPE_INTEGER:
      case ARRAY_TYPE_INTEGER_NEG:
        i++;
        if (!(i < s)) {
          goto err_malformed;
        }
        sqlite3_int64 discardInt;
        i += get_varint(&z[i], &discardInt);
        n++;
        break;

        // case ARRAY_TYPE_FLOAT:
        //   i += 9;
        //   if (!(i < s)) {
        //     goto err_malformed;
        //   }
        //   n++;
        //   break;

      default:
        goto err_malformed;
    }
  }

  sqlite3_result_int(context, n);
  return;

err_malformed:
  sqlite3_result_error(context, "malformed array", -1);
  return;
}
