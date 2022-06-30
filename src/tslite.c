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

  sqlite3_int64 a_ts = sqlite3_value_int64(argv[0]);
  double a_value = sqlite3_value_double(argv[1]);
  sqlite3_int64 norm_b_ts = sqlite3_value_int64(argv[2]) - a_ts;
  double b_value = sqlite3_value_double(argv[3]);
  sqlite3_int64 norm_t = sqlite3_value_int64(argv[4]) - a_ts;

  double res =
      a_value + ((double)norm_t / (double)norm_b_ts) * (b_value - a_value);

  sqlite3_result_double(context, res);
}

static void locf_step_func(sqlite3_context *context, int argc,
                           sqlite3_value **argv) {
  if (argc < 1) {
    sqlite3_result_error(context,
                         "too few arguments to locf() - need at least 1", -1);
    return;
  }
  sqlite3_value **last =
      sqlite3_aggregate_context(context, sizeof(sqlite3_value *));
  if (!last) {
    sqlite3_result_error_nomem(context);
    return;
  }

  if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
    if (!(*last) && argc >= 2) {
      *last = sqlite3_value_dup(argv[1]);
    }
    return;
  }

  if (*last) {
    sqlite3_value_free(*last);
  }
  *last = sqlite3_value_dup(argv[0]);
}

static void locf_final_func(sqlite3_context *context) {
  sqlite3_value **last =
      sqlite3_aggregate_context(context, sizeof(sqlite3_value *));
  if (!last) {
    sqlite3_result_error_nomem(context);
    return;
  }

  if (!(*last)) {
    sqlite3_result_null(context);
  } else {
    sqlite3_result_value(context, *last);
    sqlite3_value_free(*last);
  }
}

static void locf_value_func(sqlite3_context *context) {
  sqlite3_value **last =
      sqlite3_aggregate_context(context, sizeof(sqlite3_value *));

  if (!(*last)) {
    sqlite3_result_null(context);
  } else {
    sqlite3_result_value(context, *last);
  }
}

typedef struct {
  double first, delta;
  bool valid;
} delta_context;

static void delta_step_func(sqlite3_context *context, int argc,
                            sqlite3_value **argv) {
  if (argc < 1) {
    sqlite3_result_error(context,
                         "too few arguments to delta() - need at least 1", -1);
    return;
  }
  delta_context *delta =
      sqlite3_aggregate_context(context, sizeof(delta_context));
  if (!delta) {
    sqlite3_result_error_nomem(context);
    return;
  }

  if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
    return;
  }
  double value = sqlite3_value_double(argv[0]);
  if (!delta->valid) {
    delta->first = value;
    delta->valid = true;
    return;
  }
  double tolerance = 0;
  if (argc >= 2) {
    tolerance = sqlite3_value_double(argv[1]);
  }
  value = delta->first - value;
  if (fabs(value) > tolerance) {
    delta->delta = value;
  }
}

static void delta_final_func(sqlite3_context *context) {
  delta_context *delta =
      sqlite3_aggregate_context(context, sizeof(delta_context));
  if (!delta) {
    sqlite3_result_error_nomem(context);
    return;
  }

  if (delta->valid) {
    sqlite3_result_double(context, delta->delta);
  } else {
    sqlite3_result_null(context);
  }
}

static void delta_value_func(sqlite3_context *context) {
  delta_context *delta =
      sqlite3_aggregate_context(context, sizeof(delta_context));

  if (delta->valid) {
    sqlite3_result_double(context, delta->delta);
  } else {
    sqlite3_result_null(context);
  }
}

typedef struct {
  sqlite3_int64 changes;
  double prev;
  bool valid;
} num_changes_context;

static void num_changes_step_func(sqlite3_context *context, int argc,
                                  sqlite3_value **argv) {
  if (argc < 1) {
    sqlite3_result_error(
        context, "too few arguments to num_changes() - need at least 1", -1);
    return;
  }
  num_changes_context *num_changes =
      sqlite3_aggregate_context(context, sizeof(num_changes_context));
  if (!num_changes) {
    sqlite3_result_error_nomem(context);
    return;
  }

  if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
    return;
  }
  double value = sqlite3_value_double(argv[0]);
  if (!num_changes->valid) {
    num_changes->prev = value;
    num_changes->valid = true;
    return;
  }
  double tolerance = 0;
  if (argc >= 2) {
    tolerance = sqlite3_value_double(argv[1]);
  }
  double delta = num_changes->prev - value;
  if (fabs(delta) > tolerance) {
    num_changes->prev = value;
    num_changes->changes++;
  }
}

static void num_changes_final_func(sqlite3_context *context) {
  num_changes_context *num_changes =
      sqlite3_aggregate_context(context, sizeof(num_changes_context));
  if (!num_changes) {
    sqlite3_result_error_nomem(context);
    return;
  }

  if (num_changes->valid) {
    sqlite3_result_int64(context, num_changes->changes);
  } else {
    sqlite3_result_null(context);
  }
}

static void num_changes_value_func(sqlite3_context *context) {
  num_changes_context *num_changes =
      sqlite3_aggregate_context(context, sizeof(num_changes_context));

  if (num_changes->valid) {
    sqlite3_result_int64(context, num_changes->changes);
  } else {
    sqlite3_result_null(context);
  }
}

typedef struct {
  double prev;
  bool ticked;
  bool valid;
} tick_changes_context;

static void tick_changes_step_func(sqlite3_context *context, int argc,
                                   sqlite3_value **argv) {
  if (argc < 1) {
    sqlite3_result_error(
        context, "too few arguments to tick_changes() - need at least 1", -1);
    return;
  }
  tick_changes_context *tick_changes =
      sqlite3_aggregate_context(context, sizeof(tick_changes_context));
  if (!tick_changes) {
    sqlite3_result_error_nomem(context);
    return;
  }

  if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
    return;
  }
  double value = sqlite3_value_double(argv[0]);
  if (!tick_changes->valid) {
    tick_changes->prev = value;
    tick_changes->valid = true;
    return;
  }
  double tolerance = 0;
  if (argc >= 2) {
    tolerance = sqlite3_value_double(argv[1]);
  }
  double delta = tick_changes->prev - value;
  if (fabs(delta) > tolerance) {
    tick_changes->prev = value;
    tick_changes->ticked = true;
  } else {
    tick_changes->ticked = false;
  }
}

static void tick_changes_final_func(sqlite3_context *context) {
  tick_changes_context *tick_changes =
      sqlite3_aggregate_context(context, sizeof(tick_changes_context));
  if (!tick_changes) {
    sqlite3_result_error_nomem(context);
    return;
  }

  if (tick_changes->valid) {
    sqlite3_result_int(context, tick_changes->ticked);
  } else {
    sqlite3_result_null(context);
  }
}

static void tick_changes_value_func(sqlite3_context *context) {
  tick_changes_context *tick_changes =
      sqlite3_aggregate_context(context, sizeof(tick_changes_context));

  if (tick_changes->valid) {
    sqlite3_result_int(context, tick_changes->ticked);
  } else {
    sqlite3_result_null(context);
  }
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

  int rc = sqlite3_create_function(db, "interval", 1,
                                   SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                                   interval_func, NULL, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_function(db, "time_bucket", -1,
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

  rc = sqlite3_create_window_function(db, "locf", -1, SQLITE_UTF8, NULL,
                                      locf_step_func, locf_final_func,
                                      locf_value_func, noop_step_func, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_window_function(db, "delta", -1, SQLITE_UTF8, NULL,
                                      delta_step_func, delta_final_func,
                                      delta_value_func, noop_step_func, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_window_function(
      db, "num_changes", -1, SQLITE_UTF8, NULL, num_changes_step_func,
      num_changes_final_func, num_changes_value_func, noop_step_func, NULL);
  if (rc != SQLITE_OK) {
    return rc;
  }

  rc = sqlite3_create_window_function(
      db, "tick_changes", -1, SQLITE_UTF8, NULL, tick_changes_step_func,
      tick_changes_final_func, tick_changes_value_func, noop_step_func, NULL);
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

  rc = sqlite3_create_window_function(
      db, "array_agg", -1, SQLITE_UTF8, NULL, array_agg_step_func,
      array_agg_final_func, array_agg_value_func, noop_step_func, NULL);

  return rc;
}
