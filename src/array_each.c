#include <stdbool.h>

#include "array.h"
#include "array_buffer.h"

static int array_each_vtab_connect(sqlite3 *db, void *pAux, int argc,
                                   const char *const *argv,
                                   sqlite3_vtab **ppVtab, char **pzErr) {
  UNUSED(pAux);
  UNUSED(argc);
  UNUSED(argv);
  UNUSED(pzErr);

  array_each_vtab *vtab;
  int rc;

  rc = sqlite3_declare_vtab(db, "CREATE TABLE x(\"index\", value, type)");
#define ARRAY_EACH_VTAB_INDEX 0
#define ARRAY_EACH_VTAB_VALUE 1
#define ARRAY_EACH_VTAB_TYPE 2
  if (rc != SQLITE_OK) {
    return rc;
  }

  vtab = sqlite3_malloc(sizeof(*vtab));
  *ppVtab = (sqlite3_vtab *)vtab;
  if (!vtab) {
    return SQLITE_NOMEM;
  }
  memset(vtab, 0, sizeof(*vtab));

  return SQLITE_OK;
}

static int array_each_vtab_disconnect(sqlite3_vtab *pVtab) {
  array_each_vtab *p = (array_each_vtab *)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

static int array_each_vtab_open(sqlite3_vtab *p,
                                sqlite3_vtab_cursor **ppCursor) {
  UNUSED(p);

  array_each_vtab_cursor *cursor;
  cursor = sqlite3_malloc(sizeof(*cursor));
  if (!cursor) {
    return SQLITE_NOMEM;
  }
  memset(cursor, 0, sizeof(*cursor));
  *ppCursor = &cursor->base;
  return SQLITE_OK;
}

static int array_each_vtab_close(sqlite3_vtab_cursor *cur) {
  array_each_vtab_cursor *cursor = (array_each_vtab_cursor *)cur;
  sqlite3_free(cursor);
  return SQLITE_OK;
}

static int array_each_vtab_next(sqlite3_vtab_cursor *cur) {
  array_each_vtab_cursor *cursor = (array_each_vtab_cursor *)cur;
  int delta = array_value_advance(cursor->p, cursor->n);
  if (delta == -1) {
    return SQLITE_ERROR;
  }
  cursor->p += delta;
  cursor->n -= delta;
  cursor->row_id++;
  return SQLITE_OK;
}

static int array_each_vtab_column(sqlite3_vtab_cursor *cur,
                                  sqlite3_context *context, int i) {
  array_each_vtab_cursor *cursor = (array_each_vtab_cursor *)cur;

  switch (i) {
    case ARRAY_EACH_VTAB_INDEX:
      sqlite3_result_int64(context, cursor->row_id);
      break;

    case ARRAY_EACH_VTAB_VALUE:
      int result = array_value_decode(context, cursor->p, cursor->n);
      if (result == -1) {
        sqlite3_result_error(context, "value decode error", -1);
      }
      break;

    case ARRAY_EACH_VTAB_TYPE:
      const char *type = array_value_type(*cursor->p);
      if (!type) {
        sqlite3_result_error(context, "unknown type or malformed array", -1);
      } else {
        sqlite3_result_text(context, type, -1, SQLITE_STATIC);
      }
      break;

    default:
      return SQLITE_ERROR;
  }

  return SQLITE_OK;
}

static int array_each_vtab_rowid(sqlite3_vtab_cursor *cur,
                                 sqlite_int64 *pRowid) {
  array_each_vtab_cursor *cursor = (array_each_vtab_cursor *)cur;
  *pRowid = cursor->row_id;
  return SQLITE_OK;
}

static int array_each_vtab_eof(sqlite3_vtab_cursor *cur) {
  array_each_vtab_cursor *cursor = (array_each_vtab_cursor *)cur;
  return cursor->n <= 0;
}

static int array_each_vtab_reset(sqlite3_vtab_cursor *cur) {
  array_each_vtab_cursor *cursor = (array_each_vtab_cursor *)cur;
  array_each_vtab *vtab = (array_each_vtab *)cursor->base.pVtab;

  cursor->p = vtab->z;
  cursor->n = vtab->n;
  cursor->row_id = 0;
  return SQLITE_OK;
}

static int array_each_vtab_filter(sqlite3_vtab_cursor *cur, int idxNum,
                                  const char *idxStr, int argc,
                                  sqlite3_value **argv) {
  UNUSED(idxNum);
  UNUSED(idxStr);

  array_each_vtab_cursor *cursor = (array_each_vtab_cursor *)cur;
  array_each_vtab *vtab = (array_each_vtab *)cursor->base.pVtab;

  if (argc != 1) {
    return SQLITE_ERROR;
  }

  int s = sqlite3_value_bytes(argv[0]);
  unsigned char *z = (unsigned char *)sqlite3_value_blob(argv[0]);
  if (!z) {
    return SQLITE_NOMEM;
  }

  vtab->z = z;
  vtab->n = s;
  cursor->p = z;
  cursor->n = s;
  return SQLITE_OK;
}

// static int array_each_vtab_best_index(sqlite3_vtab *tab,
//                                       sqlite3_index_info *pIdxInfo) {
//   array_each_vtab *vtab = (array_each_vtab *)tab;
//   double estimate = (double)vtab->n / 46.5;
//   pIdxInfo->estimatedCost = estimate;
//   pIdxInfo->estimatedRows = estimate;
//   return SQLITE_OK;
// }

static int array_each_vtab_best_index(sqlite3_vtab *tab,
                                      sqlite3_index_info *pIdxInfo) {
  bool unusable = false;
  const struct sqlite3_index_constraint *constraint = pIdxInfo->aConstraint;
  for (int i = 0; i < pIdxInfo->nConstraint; i++, constraint++) {
    if (constraint->usable == 0 ||
        constraint->op != SQLITE_INDEX_CONSTRAINT_EQ) {
      unusable = true;
      break;
    }
  }
  if (unusable) {
    // Probably unusable due to multiple calls to
    // array_each_vtab_best_index (e.g. JOINs).
    return SQLITE_CONSTRAINT;
  }

  if (pIdxInfo->nConstraint != 1) {
    tab->zErrMsg = sqlite3_mprintf("array_each() expects one argument");
    return SQLITE_ERROR;
  }

  pIdxInfo->aConstraintUsage[0].argvIndex = 1;
  pIdxInfo->aConstraintUsage[0].omit = 1;
  pIdxInfo->estimatedCost = (double)10;
  pIdxInfo->estimatedRows = 1000;
  return SQLITE_OK;
}

sqlite3_module array_each_module = {
    /* iVersion    */ 0,
    /* xCreate     */ 0,
    /* xConnect    */ array_each_vtab_connect,
    /* xBestIndex  */ array_each_vtab_best_index,
    /* xDisconnect */ array_each_vtab_disconnect,
    /* xDestroy    */ 0,
    /* xOpen       */ array_each_vtab_open,
    /* xClose      */ array_each_vtab_close,
    /* xFilter     */ array_each_vtab_filter,
    /* xNext       */ array_each_vtab_next,
    /* xEof        */ array_each_vtab_eof,
    /* xColumn     */ array_each_vtab_column,
    /* xRowid      */ array_each_vtab_rowid,
    /* xUpdate     */ 0,
    /* xBegin      */ 0,
    /* xSync       */ 0,
    /* xCommit     */ 0,
    /* xRollback   */ 0,
    /* xFindMethod */ 0,
    /* xRename     */ 0,
    /* xSavepoint  */ 0,
    /* xRelease    */ 0,
    /* xRollbackTo */ 0,
    /* xShadowName */ 0,
};
