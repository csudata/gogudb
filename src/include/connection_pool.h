#ifndef PATHMAN_CONNECTION_POOL_H
#define PATHMAN_CONNECTION_POOL_H

#include "catalog/pg_user_mapping.h"
#include "foreign/foreign.h"
#include "libpq-fe.h"
/* in connection_pool.c */
extern PGconn *GoguGetConnection(UserMapping *user, bool will_prep_stmt, bool in_axct);
extern void GoguReleaseConnection(PGconn *conn);
extern unsigned int GoguGetCursorNumber(PGconn *conn);
extern unsigned int GoguGetPrepStmtNumber(PGconn *conn);
extern PGresult *Gogu_pgfdw_get_result(PGconn *conn, const char *query);
extern PGresult *Gogu_pgfdw_exec_query(PGconn *conn, const char *query);
extern void Gogu_pgfdw_report_error(int elevel, PGresult *res, PGconn *conn,
                                   bool clear, const char *sql);
extern void connectionPoolRunSQL(UserMapping *user,const char *query, bool inXact);
#endif
