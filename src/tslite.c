#define TSLITE_MAIN

#include "tslite.h"

#include <stddef.h>

#include "array.h"

static void interval_func(sqlite3_context *context, int argc,
                          sqlite3_value **argv) {
  UNUSED(argc);

  const unsigned char *str = sqlite3_value_text(argv[0]);
  int res = 0;
  int acc = 0;

  while (*str) {
    if (*str >= 0x80 || *str == ' ' || *str == '\t') {
      // Ignore unicode, spaces and tabs.
      str++;
      continue;
    }
    switch (*str) {
      case 's':
        res += acc;
        acc = 0;
        break;
      case 'm':
        res += acc * 60;
        acc = 0;
        break;
      case 'h':
        res += acc * 60 * 60;
        acc = 0;
        break;
      case 'd':
        res += acc * 60 * 60 * 24;
        acc = 0;
        break;

      default:
        if (!('0' <= *str && *str <= '9')) {
          // Not a digit, error
          sqlite3_result_error(context, "invalid interval", -1);
          return;
        }
        acc = (acc * 10) + (*str - '0');
        break;
    }
    str++;
  }

  sqlite3_result_int(context, res);
}

static void time_bucket_func(sqlite3_context *context, int argc,
                             sqlite3_value **argv) {
  UNUSED(argc);

  sqlite3_int64 width = sqlite3_value_int64(argv[0]);
  if (width < 1) {
    sqlite3_result_error(context, "invalid bucket width", -1);
    return;
  }
  sqlite3_int64 ts = sqlite3_value_int64(argv[1]);
  sqlite3_int64 bucket = (ts / width) * width;
  sqlite3_result_int64(context, bucket);
}

static void lerp_func(sqlite3_context *context, int argc,
                      sqlite3_value **argv) {
  UNUSED(argc);

  sqlite3_int64 a_ts = sqlite3_value_int64(argv[0]);
  double a_value = sqlite3_value_double(argv[1]);
  sqlite3_int64 norm_b_ts = sqlite3_value_int64(argv[2]) - a_ts;
  double b_value = sqlite3_value_double(argv[3]);
  sqlite3_int64 norm_t = sqlite3_value_int64(argv[4]) - a_ts;

  double res =
      a_value + ((double)norm_t / (double)norm_b_ts) * (b_value - a_value);

  sqlite3_result_double(context, res);
}

static void last_known_step_func(sqlite3_context *context, int argc,
                                 sqlite3_value **argv) {
  UNUSED(argc);

  if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
    return;
  }

  sqlite3_value **state =
      sqlite3_aggregate_context(context, sizeof(sqlite3_value *));
  if (!state) {
    sqlite3_result_error_nomem(context);
    return;
  }

  if (*state) {
    sqlite3_value_free(*state);
  }
  *state = sqlite3_value_dup(argv[0]);
}

static void last_known_final_func(sqlite3_context *context) {
  sqlite3_value **state =
      sqlite3_aggregate_context(context, sizeof(sqlite3_value *));
  if (!state) {
    sqlite3_result_error_nomem(context);
    return;
  }

  if (!*state) {
    sqlite3_result_null(context);
    return;
  }
  sqlite3_result_value(context, *state);
  sqlite3_value_free(*state);
}

static void last_known_value_func(sqlite3_context *context) {
  sqlite3_value **state =
      sqlite3_aggregate_context(context, sizeof(sqlite3_value *));
  if (!state) {
    sqlite3_result_error_nomem(context);
    return;
  }

  if (!*state) {
    sqlite3_result_null(context);
    return;
  }
  sqlite3_result_value(context, *state);
}

#ifdef _WIN32
__declspec(dllexport)
#endif
    int sqlite3_tslite_init(sqlite3 *db, char **pzErrMsg,
                            const sqlite3_api_routines *pApi) {
  UNUSED(pzErrMsg);

  SQLITE_EXTENSION_INIT2(pApi);

  int rc = sqlite3_create_function(db, "interval", 1,
                                   SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                                   interval_func, NULL, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_function(db, "time_bucket", 2,
                               SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                               time_bucket_func, NULL, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc =
      sqlite3_create_function(db, "lerp", 5, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                              NULL, lerp_func, NULL, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_window_function(
      db, "last_known", 1, SQLITE_UTF8, NULL, last_known_step_func,
      last_known_final_func, last_known_value_func, last_known_step_func, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_function(db, "array", -1,
                               SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                               array_func, NULL, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_function(db, "array_length", 1,
                               SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                               array_length_func, NULL, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_function(db, "array_append", -1,
                               SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                               array_append_func, NULL, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_function(db, "array_at", -1,
                               SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                               array_at_func, NULL, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_module(db, "array_each", &array_each_module, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_window_function(
      db, "array_agg", -1, SQLITE_UTF8, NULL, array_agg_step_func,
      array_agg_final_func, array_agg_value_func, array_agg_step_func, NULL);

  return rc;
}
