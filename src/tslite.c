#include <sqlite3ext.h>
#include <stddef.h>
SQLITE_EXTENSION_INIT1

#define UNUSED(x) (void)(x)

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

#ifdef _WIN32
__declspec(dllexport)
#endif
    int sqlite3_tslite_init(sqlite3 *db, char **pzErrMsg,
                            const sqlite3_api_routines *pApi) {
  UNUSED(pzErrMsg);

  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);

  rc = sqlite3_create_function(db, "interval", 1,
                               SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                               interval_func, NULL, NULL);

  if (rc == SQLITE_OK) {
    rc = sqlite3_create_function(db, "time_bucket", 2,
                                 SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                                 time_bucket_func, NULL, NULL);
  }

  return rc;
}
