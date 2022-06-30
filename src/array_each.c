#include <stdbool.h>

#include "array.h"
#include "array_buffer.h"

static int array_value_advance(unsigned char *z, int n);
static const char *array_value_type(unsigned char v);
static int array_value_decode(sqlite3_context *context, unsigned char *z,
                              int n);

static int array_each_vtab_connect(sqlite3 *db, void *pAux, int argc,
                                   const char *const *argv,
                                   sqlite3_vtab **ppVtab, char **pzErr) {
  UNUSED(pAux);
  UNUSED(argc);
  UNUSED(argv);
  UNUSED(pzErr);

  array_each_vtab *vtab;
  int rc;

  rc = sqlite3_declare_vtab(
      db, "CREATE TABLE x(\"index\", value, type, array HIDDEN)");
#define ARRAY_EACH_VTAB_ROWID -1
#define ARRAY_EACH_VTAB_INDEX 0
#define ARRAY_EACH_VTAB_VALUE 1
#define ARRAY_EACH_VTAB_TYPE 2
#define ARRAY_EACH_VTAB_ARRAY 3
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

static void array_each_vtab_advance(array_each_vtab_cursor *cur, int n) {
  int delta;
  for (int i = 0; i < n; i++) {
    delta = array_value_advance(cur->p, cur->n);
    if (delta == -1) {
      cur->n = 0;
      return;
    }
    cur->p += delta;
    cur->n -= delta;
    cur->row_id++;
  }
}

static int array_each_vtab_column(sqlite3_vtab_cursor *cur,
                                  sqlite3_context *context, int i) {
  array_each_vtab_cursor *cursor = (array_each_vtab_cursor *)cur;
  array_each_vtab *vtab = (array_each_vtab *)cursor->base.pVtab;

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

    case ARRAY_EACH_VTAB_ARRAY:
      sqlite3_result_blob(context, vtab->z, vtab->n, SQLITE_TRANSIENT);
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

static int array_each_vtab_filter(sqlite3_vtab_cursor *cur, int idxNum,
                                  const char *idxStr, int argc,
                                  sqlite3_value **argv) {
  UNUSED(idxStr);

  array_each_vtab_cursor *cursor = (array_each_vtab_cursor *)cur;
  array_each_vtab *vtab = (array_each_vtab *)cursor->base.pVtab;

  if (argc < 1) {
    return SQLITE_OK;
  }

  int n = sqlite3_value_bytes(argv[0]);
  const void *z = sqlite3_value_blob(argv[0]);
  if (!z) {
    return SQLITE_NOMEM;
  }

  vtab->z = (unsigned char *)z;
  vtab->n = n;
  cursor->p = (unsigned char *)z;
  cursor->n = n;

  if (argc == 2) {
    int i = sqlite3_value_int(argv[1]);
    if (idxNum == SQLITE_INDEX_CONSTRAINT_GT) {
      i++;
    }
    array_each_vtab_advance(cursor, i);
    if (idxNum == SQLITE_INDEX_CONSTRAINT_EQ) {
      cursor->n = array_value_advance(cursor->p, cursor->n);
    }
  }

  return SQLITE_OK;
}

static int array_each_vtab_best_index(sqlite3_vtab *vtab,
                                      sqlite3_index_info *pIdxInfo) {
  UNUSED(vtab);

  int arrayValueIdx = -1;
  int arrayOffsetIdx = -1;
  int indexConstraint = 0;
  const struct sqlite3_index_constraint *constraint = pIdxInfo->aConstraint;
  for (int i = 0; i < pIdxInfo->nConstraint; i++, constraint++) {
    if (constraint->iColumn == ARRAY_EACH_VTAB_ARRAY) {
      if (!constraint->usable) {
        // Unusable constraint on ARRAY, reject the entire plan.
        return SQLITE_CONSTRAINT;
      }
      if (constraint->op == SQLITE_INDEX_CONSTRAINT_EQ) {
        arrayValueIdx = i;
      }
    }
    if ((constraint->iColumn == ARRAY_EACH_VTAB_INDEX ||
         constraint->iColumn == ARRAY_EACH_VTAB_ROWID) &&
        constraint->usable) {
      switch (constraint->op) {
        case SQLITE_INDEX_CONSTRAINT_EQ:
          if (indexConstraint == SQLITE_INDEX_CONSTRAINT_EQ) {
            // Multiple EQ, so reset.
            arrayOffsetIdx = -1;
            continue;
          }
          break;

        case SQLITE_INDEX_CONSTRAINT_GT:
        case SQLITE_INDEX_CONSTRAINT_GE:
          // TODO: case SQLITE_INDEX_CONSTRAINT_OFFSET:
          if (arrayOffsetIdx != -1 &&
              indexConstraint < SQLITE_INDEX_CONSTRAINT_GT) {
            continue;
          }
          break;

        default:
          continue;
      }
      arrayOffsetIdx = i;
      indexConstraint = constraint->op;
    }
  }

  if (arrayValueIdx >= 0) {
    pIdxInfo->estimatedCost = 1.5;
    pIdxInfo->aConstraintUsage[arrayValueIdx].argvIndex = 1;
    pIdxInfo->aConstraintUsage[arrayValueIdx].omit = 1;

    if (arrayOffsetIdx >= 0) {
      pIdxInfo->estimatedCost = 0.5;
      pIdxInfo->aConstraintUsage[arrayOffsetIdx].argvIndex = 2;
      pIdxInfo->aConstraintUsage[arrayOffsetIdx].omit = 1;
      pIdxInfo->idxNum = indexConstraint;

      if (indexConstraint == SQLITE_INDEX_CONSTRAINT_EQ) {
        pIdxInfo->idxFlags |= SQLITE_INDEX_SCAN_UNIQUE;
        pIdxInfo->estimatedCost = 0.005;
      }
    }

    if (pIdxInfo->nOrderBy == 1) {
      struct sqlite3_index_orderby orderBy = pIdxInfo->aOrderBy[0];
      if (!orderBy.desc && (orderBy.iColumn == ARRAY_EACH_VTAB_ROWID ||
                            orderBy.iColumn == ARRAY_EACH_VTAB_INDEX)) {
        pIdxInfo->orderByConsumed = true;
        pIdxInfo->estimatedCost /= 2;
      }
    }
  }
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
