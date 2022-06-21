#ifndef TSLITE_ARRAY_H
#define TSLITE_ARRAY_H

#include "tslite.h"

void array_func(sqlite3_context *context, int argc, sqlite3_value **argv);
void array_length_func(sqlite3_context *context, int argc,
                       sqlite3_value **argv);

#endif  // TSLITE_ARRAY_H
