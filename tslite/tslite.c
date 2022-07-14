#include "tslite.h"

#include <ctype.h>
#include <stddef.h>

SQLITE_EXTENSION_INIT1

#define UNUSED(x) (void)(x)

static int strpluraleql(const unsigned char *str, const char *substr);

/*
 * interval_func(interval string) returns the parsed interval in seconds.
 * It's partially compatible with the PostgreSQL interval type.
 * The following units are supported in any case:
 *   - second(s) or s
 *   - minute(s) or m
 *   - hour(s) or h
 *   - day(s) or d
 *   - week(s) or w
 */
static void interval_func(sqlite3_context *context, int argc,
                          sqlite3_value **argv) {
  UNUSED(argc);

  int res = 0, acc = 0;
  const unsigned char *str = sqlite3_value_text(argv[0]);
  int substr;

  while (*str) {
    if (*str >= 0x80 || *str == ' ' || *str == '\t' || *str == '\n') {
      // Ignore unicode (UTF-8), spaces, tabs and newlines.
      str++;
      continue;
    }
    switch (*str) {
      // Second, seconds or 's'
      case 's':
      case 'S':
        substr = strpluraleql(str + 1, "econd");
        if (substr) {
          str += substr;
        }
        res += acc;
        acc = 0;
        break;

      // Minute, minutes or 'm'
      case 'm':
      case 'M':
        substr = strpluraleql(str + 1, "inute");
        if (substr) {
          str += substr;
        }
        res += acc * 60;
        acc = 0;
        break;

      // Hour, hours or 'h'
      case 'h':
      case 'H':
        substr = strpluraleql(str + 1, "our");
        if (substr) {
          str += substr;
        }
        res += acc * 60 * 60;
        acc = 0;
        break;

      // Day, days or 'd'
      case 'd':
      case 'D':
        substr = strpluraleql(str + 1, "ay");
        if (substr) {
          str += substr;
        }
        res += acc * 60 * 60 * 24;
        acc = 0;
        break;

      // Week, weeks or 'w'
      case 'w':
      case 'W':
        substr = strpluraleql(str + 1, "eek");
        if (substr) {
          str += substr;
        }
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

/*
 * time_bucket(
 *   bucket_width integer (interval in seconds),
 *   ts integer (timestamp as epoch)
 * )
 * returns the corresponding time bucket of the given ts, defined by
 * bucket_width.
 */
static void time_bucket_func(sqlite3_context *context, int argc,
                             sqlite3_value **argv) {
  UNUSED(argc);

  sqlite3_int64 res;
  sqlite3_int64 width = sqlite3_value_int64(argv[0]);
  sqlite3_int64 ts = sqlite3_value_int64(argv[1]);
  if (width < 2) {
    res = ts;
  } else {
    res = (ts / width) * width;
  }

  sqlite3_result_int64(context, res);
}

/*
 * lerp(
 *   a_ts integer (timestamp as epoch),
 *   a_value real,
 *   b_ts integer (timestamp as epoch),
 *   b_value real,
 *   t integer (timestamp as epoch)
 * )
 * returns the linear interpolated value at timestamp t between timestamp a
 * and b.
 */
static void lerp_func(sqlite3_context *context, int argc,
                      sqlite3_value **argv) {
  UNUSED(argc);

  double res;
  sqlite3_int64 a_ts = sqlite3_value_int64(argv[0]);
  double a_value = sqlite3_value_double(argv[1]);
  double norm_b_ts = (double)(sqlite3_value_int64(argv[2]) - a_ts);
  if (norm_b_ts == 0.0) {
    res = a_value;
  } else {
    double b_value = sqlite3_value_double(argv[3]);
    double norm_t = (double)(sqlite3_value_int64(argv[4]) - a_ts);
    res = a_value + (norm_t / norm_b_ts) * (b_value - a_value);
  }

  sqlite3_result_double(context, res);
}

/*
 * Context of the locf() window function.
 */
typedef struct {
  sqlite3_int64 windowSize, relIndex;
  sqlite3_value *last;
} locf_context;

/*
 * Steps through the given value and executes the Last Observation Carried
 * Forward (LOCF).
 * NULLs are ignored.
 */
static void locf_step_func(sqlite3_context *context, int argc,
                           sqlite3_value **argv) {
  UNUSED(argc);

  locf_context *locf = sqlite3_aggregate_context(context, sizeof(locf_context));
  if (!locf) {
    sqlite3_result_error_nomem(context);
    return;
  }

  if (sqlite3_value_type(argv[0]) != SQLITE_NULL) {
    sqlite3_value_free(locf->last);
    locf->last = sqlite3_value_dup(argv[0]);
    locf->relIndex = locf->windowSize;
  }

  locf->windowSize++;
}

/*
 * Takes the current frame into account for locf().
 */
static void locf_inverse_func(sqlite3_context *context, int argc,
                              sqlite3_value **argv) {
  UNUSED(argc);
  UNUSED(argv);

  locf_context *locf = sqlite3_aggregate_context(context, sizeof(locf_context));

  locf->relIndex--;
  locf->windowSize--;
  if (locf->relIndex < 0 && locf->last) {
    // Last observation outside of current frame, clear the value.
    sqlite3_value_free(locf->last);
    locf->last = NULL;
  }
}

/*
 * Returns the value of the Last Observation Carried Forward (LOCF) in the
 * current frame.
 */
static void locf_value_func(sqlite3_context *context) {
  locf_context *locf = sqlite3_aggregate_context(context, sizeof(locf_context));

  if (!locf || !locf->last) {
    sqlite3_result_null(context);
  } else {
    // Return the last observation in the current frame.
    sqlite3_result_value(context, locf->last);
  }
}

/*
 * Finalizes the locf() window function.
 */
static void locf_final_func(sqlite3_context *context) {
  locf_context *locf = sqlite3_aggregate_context(context, sizeof(locf_context));

  if (!locf || !locf->last) {
    sqlite3_result_null(context);
  } else {
    sqlite3_result_value(context, locf->last);
    sqlite3_value_free(locf->last);
  }
}

#ifdef _WIN32
__declspec(dllexport)
#endif
    /*
     * Initializes the tslite extension.
     */
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
      db, "time_bucket", 2,
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

  return rc;
}

/*
 * strpluraleql returns whether str starts with substr including the plural form
 * of substr (ends with s). It's case insensitive. Returns the length of the
 * actual substr (+1 if it's plural) or 0 otherwise.
 */
static int strpluraleql(const unsigned char *str, const char *substr) {
  int len = 0;
  for (;; str++, substr++, len++) {
    if (tolower(*str) != tolower(*substr)) {
      if (*substr) {
        // No match, substr has more content.
        return 0;
      }
      break;
    }
    if (!(*str)) {
      // End of string.
      return len;
    }
  }
  if (*str == 's' || *str == 'S') {
    // Plural form, increment len for consumption of 's'.
    len++;
  }
  return len;
}
