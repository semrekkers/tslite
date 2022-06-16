#ifndef TSLITE_TSLITE_H
#define TSLITE_TSLITE_H

#include <sqlite3ext.h>
#include <stddef.h>

const sqlite3_api_routines *sqlite3_api;

#define UNUSED(x) (void)(x)

static void interval_func(sqlite3_context *context, int argc,
                          sqlite3_value **argv);

static void time_bucket_func(sqlite3_context *context, int argc,
                             sqlite3_value **argv);

static void lerp_func(sqlite3_context *context, int argc, sqlite3_value **argv);

static void last_known_step_func(sqlite3_context *context, int argc,
                                 sqlite3_value **argv);

static void last_known_final_func(sqlite3_context *context);

static void last_known_value_func(sqlite3_context *context);

static void array_func(sqlite3_context *context, int argc,
                       sqlite3_value **argv);

#endif  // TSLITE_TSLITE_H
