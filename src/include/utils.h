/* ------------------------------------------------------------------------
 *
 * utils.h
 *		prototypes of various support functions
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PATHMAN_UTILS_H
#define PATHMAN_UTILS_H


#include "pathman.h"

#include "postgres.h"
#include "parser/parse_oper.h"


/*
 * Various traits.
 */
bool clause_contains_params(Node *clause);
bool is_date_type_internal(Oid typid);
bool check_security_policy_internal(Oid relid, Oid role);
bool match_expr_to_operand(const Node *expr, const Node *operand);

/*
 * Misc.
 */
Oid get_pathman_schema(void);
List * list_reverse(List *l);

/*
 * Useful functions for relations.
 */
Oid get_rel_owner(Oid relid);
char * get_rel_name_or_relid(Oid relid);
RangeVar *makeRangeVarFromRelid(Oid relid);

/*
 * Operator-related stuff.
 */
Operator get_binary_operator(char *opname, Oid arg1, Oid arg2);
void fill_type_cmp_fmgr_info(FmgrInfo *finfo, Oid type1, Oid type2);
void extract_op_func_and_ret_type(char *opname,
								  Oid type1, Oid type2,
								  Oid *op_func,
								  Oid *op_ret_type);

/*
 * Print values and cast types.
 */
char * datum_to_cstring(Datum datum, Oid typid);
Datum perform_type_cast(Datum value, Oid in_type, Oid out_type, bool *success);
Datum extract_binary_interval_from_text(Datum interval_text,
										Oid part_atttype,
										Oid *interval_type);
char ** deconstruct_text_array(Datum array, int *array_size);
RangeVar ** qualified_relnames_to_rangevars(char **relnames, size_t nrelnames);

char** getRemoteServer4Child(int *server_total);
void buildCTS4RemoteServer(StringInfoData* sql, Oid relid);
void buildCIS4RemoteServer(StringInfoData* sql, Oid relid, 
				const char *remote_schema,
				const char *remote_table);

char* get_rel_name_or_relid(Oid relid);
bool spi_run_sql(List* sql_list);
#endif /* PATHMAN_UTILS_H */
