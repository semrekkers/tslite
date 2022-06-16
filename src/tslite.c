#include "tslite.h"
SQLITE_EXTENSION_INIT1

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

  return rc;
}
