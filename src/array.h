#ifndef TSLITE_ARRAY_H
#define TSLITE_ARRAY_H

#include "tslite.h"

#define ARRAY_TYPE_NULL 0
#define ARRAY_TYPE_INTEGER 1
#define ARRAY_TYPE_INTEGER_NEG 2
#define ARRAY_TYPE_FLOAT 3
#define ARRAY_TYPE_ZERO 4
#define ARRAY_TYPE_ONE 5
#define ARRAY_TYPE_BLOB 6
#define ARRAY_TYPE_TEXT 7

static int array_value_advance(unsigned char *z, int n);
static const char *array_value_type(unsigned char v);
static int array_value_decode(sqlite3_context *context, unsigned char *z,
                              int n);

void array_func(sqlite3_context *context, int argc, sqlite3_value **argv);
void array_length_func(sqlite3_context *context, int argc,
                       sqlite3_value **argv);
void array_append_func(sqlite3_context *context, int argc,
                       sqlite3_value **argv);
void array_at_func(sqlite3_context *context, int argc, sqlite3_value **argv);

typedef struct {
  sqlite3_vtab base;
  unsigned char *z;
  int n;
} array_each_vtab;

typedef struct {
  sqlite3_vtab_cursor base;
  sqlite3_int64 row_id;
  unsigned char *p;
  int n;
} array_each_vtab_cursor;

#endif  // TSLITE_ARRAY_H
