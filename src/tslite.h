#ifndef TSLITE_TSLITE_H
#define TSLITE_TSLITE_H

#include <sqlite3ext.h>

#ifdef TSLITE_MAIN
SQLITE_EXTENSION_INIT1
extern sqlite3_module array_each_module;
#else
SQLITE_EXTENSION_INIT3
#endif

#define UNUSED(x) (void)(x)

#endif  // TSLITE_TSLITE_H
