#ifndef TSLITE_TSLITE_H
#define TSLITE_TSLITE_H

#include <sqlite3ext.h>

int sqlite3_tslite_init(sqlite3 *db, char **pzErrMsg,
                        const sqlite3_api_routines *pApi);

#endif  // TSLITE_TSLITE_H
