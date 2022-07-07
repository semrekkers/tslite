#define TSLITE_MAIN

#include "tslite.h"

#include <math.h>
#include <stdbool.h>
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
      case 'w':
        res += acc * 60 * 60 * 24 * 7;
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
  if (argc < 2) {
    sqlite3_result_error(
        context,
        "too few arguments to function time_bucket() - need at least 2", -1);
    return;
  }

  sqlite3_int64 width = sqlite3_value_int64(argv[0]);
  if (width < 1) {
    sqlite3_result_error(context, "invalid bucket width", -1);
    return;
  }
  sqlite3_int64 ts = sqlite3_value_int64(argv[1]);
  sqlite3_int64 offset = 0;
  if (argc >= 3) {
    offset = sqlite3_value_int64(argv[2]);
  }

  sqlite3_result_int64(context, ((ts / width) * width) + offset);
}

static void lerp_func(sqlite3_context *context, int argc,
                      sqlite3_value **argv) {
  UNUSED(argc);

  double res;
  sqlite3_int64 a_ts = sqlite3_value_int64(argv[0]);
  double a_value = sqlite3_value_double(argv[1]);
  double norm_b_ts = (double)(sqlite3_value_int64(argv[2]) - a_ts);
  if (norm_b_ts == 0) {
    res = a_value;
  } else {
    double b_value = sqlite3_value_double(argv[3]);
    double norm_t = (double)(sqlite3_value_int64(argv[4]) - a_ts);
    res = a_value + (norm_t / norm_b_ts) * (b_value - a_value);
  }
  sqlite3_result_double(context, res);
}

typedef struct {
  sqlite3_int64 windowSize, relIndex;
  sqlite3_value *last;
} locf_context;

static void locf_step_func(sqlite3_context *context, int argc,
                           sqlite3_value **argv) {
  UNUSED(argc);

  locf_context *locf = sqlite3_aggregate_context(context, sizeof(locf_context));
  if (!locf) {
    sqlite3_result_error_nomem(context);
    return;
  }

  if (sqlite3_value_type(argv[0]) != SQLITE_NULL) {
    locf->last = sqlite3_value_dup(argv[0]);
    locf->relIndex = locf->windowSize;
  }

  locf->windowSize++;
}

static void locf_inverse_func(sqlite3_context *context, int argc,
                              sqlite3_value **argv) {
  UNUSED(argc);
  UNUSED(argv);

  locf_context *locf = sqlite3_aggregate_context(context, sizeof(locf_context));

  locf->relIndex--;
  locf->windowSize--;
  if (locf->relIndex < 0 && locf->last) {
    sqlite3_value_free(locf->last);
    locf->last = NULL;
  }
}

static void locf_value_func(sqlite3_context *context) {
  locf_context *locf = sqlite3_aggregate_context(context, sizeof(locf_context));

  if (!locf || !locf->last) {
    sqlite3_result_null(context);
  } else {
    sqlite3_result_value(context, locf->last);
  }
}

static void locf_final_func(sqlite3_context *context) {
  locf_context *locf = sqlite3_aggregate_context(context, sizeof(locf_context));

  if (!locf || !locf->last) {
    sqlite3_result_null(context);
  } else {
    sqlite3_result_value(context, locf->last);
    sqlite3_value_free(locf->last);
  }
}

typedef struct {
  sqlite3_int64 count, last;
} change_count_context;

static void change_count_step_func(sqlite3_context *context, int argc,
                                   sqlite3_value **argv) {
  UNUSED(argc);

  change_count_context *change_count =
      sqlite3_aggregate_context(context, sizeof(change_count_context));
  if (!change_count) {
    sqlite3_result_error_nomem(context);
    return;
  }

  if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
    return;
  }
  sqlite3_int64 value = sqlite3_value_int64(argv[0]);
  if (change_count->count) {
    if (value != change_count->last) {
      change_count->last = value;
      change_count->count++;
    }
  } else {
    change_count->last = value;
    change_count->count = 1;
  }
}

static void change_count_value_func(sqlite3_context *context) {
  change_count_context *change_count =
      sqlite3_aggregate_context(context, sizeof(change_count_context));
  if (!change_count) {
    sqlite3_result_null(context);
    return;
  }
  sqlite3_result_int64(context, change_count->count);
}

static void noop_step_func(sqlite3_context *context, int argc,
                           sqlite3_value **argv) {
  UNUSED(context);
  UNUSED(argc);
  UNUSED(argv);
}

#ifdef _WIN32
__declspec(dllexport)
#endif
    int sqlite3_tslite_init(sqlite3 *db, char **pzErrMsg,
                            const sqlite3_api_routines *pApi) {
  UNUSED(pzErrMsg);

  SQLITE_EXTENSION_INIT2(pApi);

  int rc = sqlite3_create_function(
      db, "interval", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS,
      NULL, interval_func, NULL, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_function(
      db, "time_bucket", -1,
      SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS, NULL,
      time_bucket_func, NULL, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_function(
      db, "lerp", 5, SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS,
      NULL, lerp_func, NULL, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_window_function(db, "locf", 1, SQLITE_UTF8, NULL,
                                      locf_step_func, locf_final_func,
                                      locf_value_func, locf_inverse_func, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_window_function(
      db, "change_count", 1, SQLITE_UTF8, NULL, change_count_step_func,
      change_count_value_func, change_count_value_func, noop_step_func, NULL);
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

  rc = sqlite3_create_function(db, "array_at", 2,
                               SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                               array_at_func, NULL, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_module(db, "array_each", &array_each_module, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_window_function(db, "array_agg", -1, SQLITE_UTF8, NULL,
                                      array_agg_step_func, array_agg_final_func,
                                      NULL, NULL, NULL);

  return rc;
}
