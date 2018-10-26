/* ------------------------------------------------------------------------
 *
 * utils.c
 *		definitions of various support functions
 *
 * Copyright (c) 2016, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */

#include "utils.h"

#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_am.h"
#include "catalog/pg_opclass.h"

#include "commands/extension.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"

#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/ruleutils.h"
#include "executor/spi.h"

#if PG_VERSION_NUM >= 100000
#include "utils/regproc.h"
#endif


typedef struct _tableAttributeInfo
{
	/*
	 * These fields are computed only if we decide the table is interesting
	 * (it's either a table to dump, or a direct parent of a dumpable table).
	 */
	int		numatts;		/* number of attributes */
	char	  	**attnames;		/* the attribute names */
	char	  	**atttypnames;	/* attribute type names */
	int		*atttypmod;		/* type-specific type modifiers */
	int		*attstattarget;	/* attribute statistics targets */
	char	   	*attstorage;		/* attribute storage scheme */
	char	   	*typstorage;		/* type storage scheme */
	bool	   	*attisdropped;	/* true if attr is dropped; don't dump it */
	char	   	*attidentity;
	int		*attlen;			/* attribute length, used by binary_upgrade */
	char	   	*attalign;		/* attribute align, used by binary_upgrade */
	bool	   	*attislocal;		/* true if attr has local definition */
	char	  	**attoptions;		/* per-attribute options */
	Oid		*attcollation;	/* per-attribute collation selection */
	bool	   	*notnull;		/* NOT NULL constraints on attributes */
	bool	   	*inhNotNull;		/* true if NOT NULL is inherited */
	char	   	**attrdefs; /* DEFAULT expressions */

} MetaInfo;

static MetaInfo* getTableAttrs(Oid relid);
static void getAttrsCollation(Oid cid, char **collname, char **ns_name);
static bool looks_like_function(Node *node);
static char* get_relation_name(Oid relid);
static void get_opclass_name(Oid opclass, Oid actual_datatype, StringInfo buf);
static char* flatten_reloptions(Oid relid);
static void simple_quote_literal(StringInfo buf, const char *val);

static const Node *
drop_irrelevant_expr_wrappers(const Node *expr)
{
	switch (nodeTag(expr))
	{
		/* Strip relabeling */
		case T_RelabelType:
			return (const Node *) ((const RelabelType *) expr)->arg;

		/* no special actions required */
		default:
			return expr;
	}
}

static bool
clause_contains_params_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Param))
		return true;

	return expression_tree_walker(node,
								  clause_contains_params_walker,
								  context);
}

/*
 * Check whether clause contains PARAMs or not.
 */
bool
clause_contains_params(Node *clause)
{
	return expression_tree_walker(clause,
								  clause_contains_params_walker,
								  NULL);
}

/*
 * Check if this is a "date"-related type.
 */
bool
is_date_type_internal(Oid typid)
{
	return typid == TIMESTAMPOID ||
		   typid == TIMESTAMPTZOID ||
		   typid == DATEOID;
}

/*
 * Check if user can alter/drop specified relation. This function is used to
 * make sure that current user can change pg_pathman's config. Returns true
 * if user can manage relation, false otherwise.
 *
 * XXX currently we just check if user is a table owner. Probably it's
 * better to check user permissions in order to let other users participate.
 */
bool
check_security_policy_internal(Oid relid, Oid role)
{
	Oid owner;

	/* Superuser is allowed to do anything */
	if (superuser())
		return true;

	/* Fetch the owner */
	owner = get_rel_owner(relid);

	/*
	 * Sometimes the relation doesn't exist anymore but there is still
	 * a record in config. For instance, it happens in DDL event trigger.
	 * Still we should be able to remove this record.
	 */
	if (owner == InvalidOid)
		return true;

	/* Check if current user is the owner of the relation */
	if (owner != role)
		return false;

	return true;
}

/* Compare clause operand with expression */
bool
match_expr_to_operand(const Node *expr, const Node *operand)
{
	expr = drop_irrelevant_expr_wrappers(expr);
	operand = drop_irrelevant_expr_wrappers(operand);

	/* compare expressions and return result right away */
	return equal(expr, operand);
}


/*
 * Return pg_pathman schema's Oid or InvalidOid if that's not possible.
 */
Oid
get_pathman_schema(void)
{
	Oid				result;
	Relation		rel;
	SysScanDesc		scandesc;
	HeapTuple		tuple;
	ScanKeyData		entry[1];
	Oid				ext_schema;

	/* It's impossible to fetch pg_pathman's schema now */
	if (!IsTransactionState())
		return InvalidOid;

	ext_schema = get_extension_oid("gogudb", true);
	if (ext_schema == InvalidOid)
		return InvalidOid; /* exit if pg_pathman does not exist */

	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ext_schema));

	rel = heap_open(ExtensionRelationId, AccessShareLock);
	scandesc = systable_beginscan(rel, ExtensionOidIndexId, true,
								  NULL, 1, entry);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = ((Form_pg_extension) GETSTRUCT(tuple))->extnamespace;
	else
		result = InvalidOid;

	systable_endscan(scandesc);

	heap_close(rel, AccessShareLock);

	return result;
}

List *
list_reverse(List *l)
{
	List *result = NIL;
	ListCell *lc;

	foreach (lc, l)
	{
		result = lcons(lfirst(lc), result);
	}
	return result;
}


/*
 * Get relation owner.
 */
Oid
get_rel_owner(Oid relid)
{
	HeapTuple	tp;
	Oid 		owner;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);

		owner = reltup->relowner;
		ReleaseSysCache(tp);

		return owner;
	}

	return InvalidOid;
}

/*
 * Try to get relname or at least relid as cstring.
 */
char *
get_rel_name_or_relid(Oid relid)
{
	char *relname = get_rel_name(relid);

	if (!relname)
		return DatumGetCString(DirectFunctionCall1(oidout, ObjectIdGetDatum(relid)));

	return relname;
}

RangeVar *
makeRangeVarFromRelid(Oid relid)
{
	char *relname = get_rel_name(relid);
	char *nspname = get_namespace_name(get_rel_namespace(relid));

	return makeRangeVar(nspname, relname, -1);
}



/*
 * Try to find binary operator.
 *
 * Returns operator function's Oid or throws an ERROR on InvalidOid.
 */
Operator
get_binary_operator(char *oprname, Oid arg1, Oid arg2)
{
	Operator op;

	op = compatible_oper(NULL, list_make1(makeString(oprname)),
						 arg1, arg2, true, -1);

	if (!op)
		elog(ERROR, "cannot find operator %s(%s, %s)",
			 oprname,
			 format_type_be(arg1),
			 format_type_be(arg2));

	return op;
}

/*
 * Get BTORDER_PROC for two types described by Oids.
 */
void
fill_type_cmp_fmgr_info(FmgrInfo *finfo, Oid type1, Oid type2)
{
	Oid				cmp_proc_oid;
	TypeCacheEntry *tce_1,
				   *tce_2;

	/* Check type compatibility */
	if (IsBinaryCoercible(type1, type2))
		type1 = type2;

	else if (IsBinaryCoercible(type2, type1))
		type2 = type1;

	tce_1 = lookup_type_cache(type1, TYPECACHE_BTREE_OPFAMILY);
	tce_2 = lookup_type_cache(type2, TYPECACHE_BTREE_OPFAMILY);

	/* Both types should belong to the same opfamily */
	if (tce_1->btree_opf != tce_2->btree_opf)
		goto fill_type_cmp_fmgr_info_error;

	cmp_proc_oid = get_opfamily_proc(tce_1->btree_opf,
									 tce_1->btree_opintype,
									 tce_2->btree_opintype,
									 BTORDER_PROC);

	/* No such function, emit ERROR */
	if (!OidIsValid(cmp_proc_oid))
		goto fill_type_cmp_fmgr_info_error;

	/* Fill FmgrInfo struct */
	fmgr_info(cmp_proc_oid, finfo);

	return; /* everything is OK */

/* Handle errors (no such function) */
fill_type_cmp_fmgr_info_error:
	elog(ERROR, "missing comparison function for types %s & %s",
		 format_type_be(type1), format_type_be(type2));
}

/*
 * Fetch binary operator by name and return it's function and ret type.
 */
void
extract_op_func_and_ret_type(char *opname,
							 Oid type1, Oid type2,
							 Oid *op_func,		/* ret value #1 */
							 Oid *op_ret_type)	/* ret value #2 */
{
	Operator op;

	/* Get "move bound operator" descriptor */
	op = get_binary_operator(opname, type1, type2);
	Assert(op);

	*op_func = oprfuncid(op);
	*op_ret_type = ((Form_pg_operator) GETSTRUCT(op))->oprresult;

	/* Don't forget to release system cache */
	ReleaseSysCache(op);
}



/*
 * Get CSTRING representation of Datum using the type Oid.
 */
char *
datum_to_cstring(Datum datum, Oid typid)
{
	char	   *result;
	HeapTuple	tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));

	if (HeapTupleIsValid(tup))
	{
		Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tup);
		result = OidOutputFunctionCall(typtup->typoutput, datum);
		ReleaseSysCache(tup);
	}
	else
		result = pstrdup("[error]");

	return result;
}



/*
 * Try casting value of type 'in_type' to 'out_type'.
 *
 * This function might emit ERROR.
 */
Datum
perform_type_cast(Datum value, Oid in_type, Oid out_type, bool *success)
{
	CoercionPathType	ret;
	Oid					castfunc = InvalidOid;

	/* Speculative success */
	if (success) *success = true;

	/* Fast and trivial path */
	if (in_type == out_type)
		return value;

	/* Check that types are binary coercible */
	if (IsBinaryCoercible(in_type, out_type))
		return value;

	/* If not, try to perform a type cast */
	ret = find_coercion_pathway(out_type, in_type,
								COERCION_EXPLICIT,
								&castfunc);

	/* Handle coercion paths */
	switch (ret)
	{
		/* There's a function */
		case COERCION_PATH_FUNC:
			{
				/* Perform conversion */
				Assert(castfunc != InvalidOid);
				return OidFunctionCall1(castfunc, value);
			}

		/* Types are binary compatible (no implicit cast) */
		case COERCION_PATH_RELABELTYPE:
			{
				/* We don't perform any checks here */
				return value;
			}

		/* TODO: implement these casts if needed */
		case COERCION_PATH_ARRAYCOERCE:
		case COERCION_PATH_COERCEVIAIO:

		/* There's no cast available */
		case COERCION_PATH_NONE:
		default:
			{
				/* Oops, something is wrong */
				if (success)
					*success = false;
				else
					elog(ERROR, "cannot cast %s to %s",
						 format_type_be(in_type),
						 format_type_be(out_type));

				return (Datum) 0;
			}
	}
}

/*
 * Convert interval from TEXT to binary form using partitioninig expression type.
 */
Datum
extract_binary_interval_from_text(Datum interval_text,	/* interval as TEXT */
								  Oid part_atttype,		/* expression type */
								  Oid *interval_type)	/* ret value #1 */
{
	Datum		interval_binary;
	const char *interval_cstring;

	interval_cstring = TextDatumGetCString(interval_text);

	/* If 'part_atttype' is a *date type*, cast 'range_interval' to INTERVAL */
	if (is_date_type_internal(part_atttype))
	{
		int32	interval_typmod = PATHMAN_CONFIG_interval_typmod;

		/* Convert interval from CSTRING to internal form */
		interval_binary = DirectFunctionCall3(interval_in,
											  CStringGetDatum(interval_cstring),
											  ObjectIdGetDatum(InvalidOid),
											  Int32GetDatum(interval_typmod));
		if (interval_type)
			*interval_type = INTERVALOID;
	}
	/* Otherwise cast it to the partitioned column's type */
	else
	{
		HeapTuple	htup;
		Oid			typein_proc = InvalidOid;

		htup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(part_atttype));
		if (HeapTupleIsValid(htup))
		{
			typein_proc = ((Form_pg_type) GETSTRUCT(htup))->typinput;
			ReleaseSysCache(htup);
		}
		else
			elog(ERROR, "cannot find input function for type %u", part_atttype);

		/*
		 * Convert interval from CSTRING to 'prel->ev_type'.
		 *
		 * Note: We pass 3 arguments in case
		 * 'typein_proc' also takes Oid & typmod.
		 */
		interval_binary = OidFunctionCall3(typein_proc,
										   CStringGetDatum(interval_cstring),
										   ObjectIdGetDatum(part_atttype),
										   Int32GetDatum(-1));
		if (interval_type)
			*interval_type = part_atttype;
	}

	return interval_binary;
}

/* Convert Datum into CSTRING array */
char **
deconstruct_text_array(Datum array, int *array_size)
{
	ArrayType  *array_ptr = DatumGetArrayTypeP(array);
	int16		elemlen;
	bool		elembyval;
	char		elemalign;

	Datum	   *elem_values;
	bool	   *elem_nulls;

	int			arr_size = 0;

	/* Check type invariant */
	Assert(ARR_ELEMTYPE(array_ptr) == TEXTOID);

	/* Check number of dimensions */
	if (ARR_NDIM(array_ptr) > 1)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("array should contain only 1 dimension")));

	get_typlenbyvalalign(ARR_ELEMTYPE(array_ptr),
						 &elemlen, &elembyval, &elemalign);

	deconstruct_array(array_ptr,
					  ARR_ELEMTYPE(array_ptr),
					  elemlen, elembyval, elemalign,
					  &elem_values, &elem_nulls, &arr_size);

	/* If there are actual values, convert them into CSTRINGs */
	if (arr_size > 0)
	{
		char  **strings = palloc(arr_size * sizeof(char *));
		int		i;

		for (i = 0; i < arr_size; i++)
		{
			if (elem_nulls[i])
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("array should not contain NULLs")));

			strings[i] = TextDatumGetCString(elem_values[i]);
		}

		/* Return an array and it's size */
		*array_size = arr_size;
		return strings;
	}
	/* Else emit ERROR */
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("array should not be empty")));

	/* Keep compiler happy */
	return NULL;
}

/*
 * Convert schema qualified relation names array to RangeVars array
 */
RangeVar **
qualified_relnames_to_rangevars(char **relnames, size_t nrelnames)
{
	RangeVar  **rangevars = NULL;
	int			i;

	/* Convert partition names into RangeVars */
	if (relnames)
	{
		rangevars = palloc(sizeof(RangeVar) * nrelnames);
		for (i = 0; i < nrelnames; i++)
		{
			List *nl = stringToQualifiedNameList(relnames[i]);

			rangevars[i] = makeRangeVarFromNameList(nl);
		}
	}

	return rangevars;
}

/*
 *  *      Run SQL via SPI
 *   * */
bool spi_run_sql(List* sql_list)
{
        ListCell   *cell;
        bool result = true;
        if (SPI_connect() != SPI_OK_CONNECT)
                elog(ERROR, "could not connect using SPI");
                
        foreach(cell, sql_list) 
        {
                char* sql = strVal(lfirst(cell));
                if (SPI_execute(sql, false, 0) < 0)     {
                        SPI_finish();
                        elog(ERROR, "could not execute :%s using SPI", sql);
                }
        }
        SPI_finish();
        return result;
}


static 
MetaInfo* getTableAttrs(Oid relid)
{
	int			j;
	StringInfoData 		sql;
	int			i_attnum;
	int			i_attname;
	int			i_atttypname;
	int			i_atttypmod;
	int			i_attstattarget;
	int			i_attstorage;
	int			i_typstorage;
	int			i_attnotnull;
	int			i_atthasdef;
	int			i_attidentity;
	int			i_attisdropped;
	int			i_attlen;
	int			i_attalign;
	int			i_attislocal;
	int			i_attoptions;
	int			i_attcollation;
	int	   		res;
	int			ntups;
	bool			hasdefaults;
	MetaInfo  		*tbinfo;
	SPITupleTable		*spi_tuptable;
	TupleDesc       	spi_tupdesc;
	HeapTuple       	spi_tuple;
	char			*tmp;

      	if (SPI_connect() != SPI_OK_CONNECT)
                elog(ERROR, "could not connect using SPI");

	tbinfo = SPI_palloc(sizeof(MetaInfo));
	initStringInfo(&sql);
#if PG_VERSION_NUM >= 100000
	/*
	 * attidentity is new in version 10.
	 */
	appendStringInfo(&sql, "SELECT a.attnum, a.attname, a.atttypmod, "
				"a.attstattarget, a.attstorage, t.typstorage, "
				"a.attnotnull, a.atthasdef, a.attisdropped, "
				"a.attlen, a.attalign, a.attislocal, "
				"pg_catalog.format_type(t.oid,a.atttypmod) AS atttypname, "
				"array_to_string(a.attoptions, ', ') AS attoptions, "
				"CASE WHEN a.attcollation <> t.typcollation "
				"THEN a.attcollation ELSE 0 END AS attcollation, "
				"a.attidentity "
				"FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_type t "
				"ON a.atttypid = t.oid "
				"WHERE a.attrelid = '%u'::pg_catalog.oid "
				"AND a.attnum > 0::pg_catalog.int2 "
				"ORDER BY a.attnum",
				relid);
#elif  PG_VERSION_NUM >= 90500
	/*
	 * for PG 9.5 above
	 */
	appendStringInfo(&sql, "SELECT a.attnum, a.attname, a.atttypmod, "
				"a.attstattarget, a.attstorage, t.typstorage, "
				"a.attnotnull, a.atthasdef, a.attisdropped, "
				"a.attlen, a.attalign, a.attislocal, "
				"pg_catalog.format_type(t.oid,a.atttypmod) AS atttypname, "
				"array_to_string(a.attoptions, ', ') AS attoptions, "
				"CASE WHEN a.attcollation <> t.typcollation "
				"THEN a.attcollation ELSE 0 END AS attcollation "
				"FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_type t "
				"ON a.atttypid = t.oid "
				"WHERE a.attrelid = '%u'::pg_catalog.oid "
				"AND a.attnum > 0::pg_catalog.int2 "
				"ORDER BY a.attnum",
				relid);
#else
	elog(ERROR, "unsupport PG version, quit");
#endif
                
         
        res = SPI_execute(sql.data, true, 0);
	ntups = SPI_processed;
	if (res != SPI_OK_SELECT || ntups == 0) {
		SPI_finish();
		elog(ERROR, "could not execute :%s using SPI", sql.data);
        }

        spi_tuptable = SPI_tuptable;
        spi_tupdesc = spi_tuptable->tupdesc;

	i_attnum = SPI_fnumber(spi_tupdesc, "attnum");
	i_attname = SPI_fnumber(spi_tupdesc, "attname");
	i_atttypname = SPI_fnumber(spi_tupdesc, "atttypname");
	i_atttypmod = SPI_fnumber(spi_tupdesc, "atttypmod");
	i_attstattarget = SPI_fnumber(spi_tupdesc, "attstattarget");
	i_attstorage = SPI_fnumber(spi_tupdesc, "attstorage");
	i_typstorage = SPI_fnumber(spi_tupdesc, "typstorage");
	i_attnotnull = SPI_fnumber(spi_tupdesc, "attnotnull");
	i_atthasdef = SPI_fnumber(spi_tupdesc, "atthasdef");
	i_attidentity = SPI_fnumber(spi_tupdesc, "attidentity");
	i_attisdropped = SPI_fnumber(spi_tupdesc, "attisdropped");
	i_attlen = SPI_fnumber(spi_tupdesc, "attlen");
	i_attalign = SPI_fnumber(spi_tupdesc, "attalign");
	i_attislocal = SPI_fnumber(spi_tupdesc, "attislocal");
	i_attoptions = SPI_fnumber(spi_tupdesc, "attoptions");
	i_attcollation = SPI_fnumber(spi_tupdesc, "attcollation");

	tbinfo->numatts = ntups;
	tbinfo->attnames = (char **)SPI_palloc(ntups * sizeof(char *));
	tbinfo->atttypnames = (char **)SPI_palloc(ntups * sizeof(char *));
	tbinfo->atttypmod = (int *) SPI_palloc(ntups * sizeof(int));
	tbinfo->attstattarget = (int *) SPI_palloc(ntups * sizeof(int));
	tbinfo->attstorage = (char *) SPI_palloc(ntups * sizeof(char));
	tbinfo->typstorage = (char *) SPI_palloc(ntups * sizeof(char));
	tbinfo->attidentity = (char *) SPI_palloc(ntups * sizeof(char));
	tbinfo->attisdropped = (bool *) SPI_palloc(ntups * sizeof(bool));
	tbinfo->attlen = (int *) SPI_palloc(ntups * sizeof(int));
	tbinfo->attalign = (char *) SPI_palloc(ntups * sizeof(char));
	tbinfo->attislocal = (bool *) SPI_palloc(ntups * sizeof(bool));
	tbinfo->attoptions = (char **) SPI_palloc(ntups * sizeof(char *));
	tbinfo->attcollation = (Oid *) SPI_palloc(ntups * sizeof(Oid));
	tbinfo->notnull = (bool *) SPI_palloc(ntups * sizeof(bool));
	tbinfo->inhNotNull = (bool *) SPI_palloc(ntups * sizeof(bool));
	tbinfo->attrdefs = (char **) SPI_palloc(ntups * sizeof(char *));
	hasdefaults = false;
	for (j = 0; j < ntups; j++)
	{
		spi_tuple =  spi_tuptable->vals[j];
		if (j + 1 != atoi( SPI_getvalue(spi_tuple, spi_tupdesc, i_attnum)))
			elog(ERROR, "invalid column numbering in table \"%u\"\n", relid);

		tmp = SPI_getvalue(spi_tuple, spi_tupdesc, i_attname);
		tbinfo->attnames[j] = SPI_palloc(strlen(tmp)+1);
		strcpy(tbinfo->attnames[j], tmp);
		tbinfo->attnames[j][strlen(tmp)] = 0;

		tmp =  SPI_getvalue(spi_tuple, spi_tupdesc, i_atttypname);
		tbinfo->atttypnames[j] = SPI_palloc(strlen(tmp)+1);
		strcpy(tbinfo->atttypnames[j], tmp);
		tbinfo->atttypnames[j][strlen(tmp)] = 0;

		tbinfo->atttypmod[j] = atoi(SPI_getvalue(spi_tuple, spi_tupdesc, i_atttypmod));
		tbinfo->attstattarget[j] = atoi(SPI_getvalue(spi_tuple, spi_tupdesc, i_attstattarget));
		tbinfo->attstorage[j] = *(SPI_getvalue(spi_tuple, spi_tupdesc, i_attstorage));
		tbinfo->typstorage[j] = *(SPI_getvalue(spi_tuple, spi_tupdesc, i_typstorage));
		tbinfo->attidentity[j] = (i_attidentity >= 0 ? *SPI_getvalue(spi_tuple, spi_tupdesc, i_attidentity) : '\0');
		tbinfo->attisdropped[j] = SPI_getvalue(spi_tuple, spi_tupdesc, i_attisdropped)[0] == 't';
		tbinfo->attlen[j] = atoi(SPI_getvalue(spi_tuple, spi_tupdesc, i_attlen));
		tbinfo->attalign[j] = *(SPI_getvalue(spi_tuple, spi_tupdesc, i_attalign));
		tbinfo->attislocal[j] = SPI_getvalue(spi_tuple, spi_tupdesc, i_attislocal)[0] == 't';
		tbinfo->notnull[j] = SPI_getvalue(spi_tuple, spi_tupdesc, i_attnotnull)[0] == 't';

		tmp =  SPI_getvalue(spi_tuple, spi_tupdesc, i_attoptions);
		if (tmp) {
			tbinfo->attoptions[j] = SPI_palloc(strlen(tmp)+1);
			strcpy(tbinfo->attoptions[j], tmp);
			tbinfo->attoptions[j][strlen(tmp)] = 0;
		}
#if PG_VERSION_NUM >= 100000
		tbinfo->attcollation[j] = atooid(SPI_getvalue(spi_tuple, spi_tupdesc, i_attcollation));
#else 
		tbinfo->attcollation[j] = (Oid)strtoul(SPI_getvalue(spi_tuple, spi_tupdesc, i_attcollation), NULL, 10);
#endif
		tbinfo->attrdefs[j] = NULL; /* fix below */
		if (SPI_getvalue(spi_tuple, spi_tupdesc, i_atthasdef)[0] == 't')
			hasdefaults = true;
		/* these flags will be set in flagInhAttrs() */
		tbinfo->inhNotNull[j] = false;
	}


	/*
	 * Get info about column defaults
	 */
	if (hasdefaults)
	{
		int numDefaults, i_adnum, i_adsrc;
		resetStringInfo(&sql);

		appendStringInfo(&sql, "SELECT tableoid, oid, adnum, "
					"pg_catalog.pg_get_expr(adbin, adrelid) AS adsrc "
					"FROM pg_catalog.pg_attrdef "
					"WHERE adrelid = '%u'::pg_catalog.oid",
					relid);

        	res = SPI_execute(sql.data, true, 0);
		numDefaults = SPI_processed;
		if (res != SPI_OK_SELECT || ntups == 0) {
			SPI_finish();
			elog(ERROR, "could not execute :%s using SPI", sql.data);
        	}

        	spi_tuptable = SPI_tuptable;
        	spi_tupdesc = spi_tuptable->tupdesc;
		i_adnum = SPI_fnumber(spi_tupdesc, "adnum");
		i_adsrc = SPI_fnumber(spi_tupdesc, "adsrc");
		for (j = 0; j < numDefaults; j++)
		{
			int			adnum;

			spi_tuple =  spi_tuptable->vals[j];
			adnum = atoi( SPI_getvalue(spi_tuple, spi_tupdesc, i_adnum));

			if (adnum <= 0 || adnum > ntups)
				elog(ERROR, "invalid adnum value %d for table \"%d\"\n",
								  adnum, relid);

			/*
			 * dropped columns shouldn't have defaults, but just in case,
			 * ignore 'em
			 */
			if (tbinfo->attisdropped[adnum - 1])
				continue;

			tmp =  SPI_getvalue(spi_tuple, spi_tupdesc, i_adsrc);
			if (tmp) {
				tbinfo->attrdefs[adnum - 1] = SPI_palloc(strlen(tmp)+1);
				strcpy(tbinfo->attrdefs[adnum - 1], tmp);
				tbinfo->attrdefs[adnum - 1][strlen(tmp)] = 0;
			}
		}

	}

	SPI_finish();
	return tbinfo;
}
  
char** 
getRemoteServer4Child(int *server_total)
{
	char 			*tmp = NULL, **server_array = NULL;
	char	 		*sql = "select srvname from fdw_server";
	int 			i_server, res, i;
	SPITupleTable		*spi_tuptable;
	TupleDesc       	spi_tupdesc;
	HeapTuple       	spi_tuple;


	if (SPI_connect() != SPI_OK_CONNECT)
                elog(ERROR, "could not connect using SPI");
                
      
        res = SPI_execute(sql, true, 0);
	*server_total = SPI_processed;
	if (res != SPI_OK_SELECT || *server_total == 0) {
		SPI_finish();
		elog(ERROR, "could not execute :%s using SPI", sql);
        }

        spi_tuptable = SPI_tuptable;
        spi_tupdesc = spi_tuptable->tupdesc;
	server_array = (char **) SPI_palloc(sizeof(char*)* SPI_processed);
	i_server = SPI_fnumber(spi_tupdesc, "srvname");
	for (i = 0; i < SPI_processed; i++) {
		spi_tuple =  spi_tuptable->vals[i]; 
		tmp = SPI_getvalue(spi_tuple, spi_tupdesc, i_server);
		server_array[i] =  (char *) SPI_palloc(strlen(tmp)+1);
		strcpy(server_array[i], tmp);
		server_array[i][strlen(tmp)] = 0;
	}
	SPI_finish();
	return server_array; 
}

static bool
looks_like_function(Node *node)
{
        if (node == NULL)
                return false;                   /* probably shouldn't happen */
        switch (nodeTag(node))
        {
                case T_FuncExpr:
                        /* OK, unless it's going to deparse as a cast */
                        return (((FuncExpr *) node)->funcformat == COERCE_EXPLICIT_CALL);
                case T_NullIfExpr:
                case T_CoalesceExpr:
                case T_MinMaxExpr:
#if PG_VERSION_NUM >= 100000
                case T_SQLValueFunction:
#endif
                case T_XmlExpr:
                        /* these are all accepted by func_expr_common_subexpr */
                        return true;
                default:
                        break;
        }
        return false;
}

static char *
get_relation_name(Oid relid)
{
        char       *relname = get_rel_name(relid);

        if (!relname)
                elog(ERROR, "cache lookup failed for relation %u", relid);
        return relname;
}

static void
get_opclass_name(Oid opclass, Oid actual_datatype, StringInfo buf)
{
	HeapTuple       ht_opc;
	Form_pg_opclass opcrec;
	char       *opcname;
	char       *nspname;

	ht_opc = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
    if (!HeapTupleIsValid(ht_opc))
		elog(ERROR, "cache lookup failed for opclass %u", opclass);

	opcrec = (Form_pg_opclass) GETSTRUCT(ht_opc);

	if (!OidIsValid(actual_datatype) ||
			GetDefaultOpClass(actual_datatype, opcrec->opcmethod) != opclass)
	{
		/* Okay, we need the opclass name.  Do we need to qualify it? */
		opcname = NameStr(opcrec->opcname);
		if (OpclassIsVisible(opclass))
			appendStringInfo(buf, " %s", quote_identifier(opcname));
		else
		{
			nspname = get_namespace_name(opcrec->opcnamespace);
			appendStringInfo(buf, " %s.%s",
            quote_identifier(nspname),
            quote_identifier(opcname));
		}
	}
	ReleaseSysCache(ht_opc);
}

static void
simple_quote_literal(StringInfo buf, const char *val)
{
	const char *valptr;

	/*
	 ** We form the string literal according to the prevailing setting of
	 ** standard_conforming_strings; we never use E''. User is responsible for
	 ** making sure result is used correctly.
	 **/
	appendStringInfoChar(buf, '\'');
	for (valptr = val; *valptr; valptr++)
	{
		char            ch = *valptr;
		if (SQL_STR_DOUBLE(ch, false))
			appendStringInfoChar(buf, ch);

		appendStringInfoChar(buf, ch);
	}
	appendStringInfoChar(buf, '\'');
}

/*
 *  * Generate a C string representing a relation's reloptions, or NULL if none.
 *   */
static char *
flatten_reloptions(Oid relid)
{
	char       *result = NULL;
	HeapTuple       tuple;
	Datum           reloptions;
	bool            isnull;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);
	if (!isnull)
	{
		StringInfoData buf;
		Datum      *options;
		int                     noptions;
		int                     i;

		initStringInfo(&buf);
		deconstruct_array(DatumGetArrayTypeP(reloptions),
							TEXTOID, -1, false, 'i',
							&options, NULL, &noptions);
		for (i = 0; i < noptions; i++)
		{
			char       *option = TextDatumGetCString(options[i]);
			char       *name;
			char       *separator;
			char       *value;

			/*
 			 ** Each array element should have the form name=value.  If the "="
			 ** is missing for some reason, treat it like an empty value.
			 **/
			name = option;
			separator = strchr(option, '=');
			if (separator)
			{
				*separator = '\0';
				value = separator + 1;
			}
			else
				value = "";

			if (i > 0)
				appendStringInfoString(&buf, ", ");

			appendStringInfo(&buf, "%s=", quote_identifier(name));
			if (quote_identifier(value) == value)
				appendStringInfoString(&buf, value);
			else
				simple_quote_literal(&buf, value);

			pfree(option);
		}
	
		result = buf.data;
	}

	ReleaseSysCache(tuple);

	return result;
}
/**
 *  build sql to create index on remote server
 * */
void 
buildCIS4RemoteServer(StringInfoData* sql, Oid relid,
				const char *remote_schema, const char* remote_table)
{
	Relation	indRelation;
	HeapScanDesc	scan;
	ScanKeyData	entry;
	HeapTuple	indexTuple;
	
	indRelation = heap_open(IndexRelationId, AccessShareLock);
	ScanKeyInit(&entry, Anum_pg_index_indrelid, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(relid));

	scan = heap_beginscan_catalog(indRelation, 1, &entry);

	while ((indexTuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
		Datum		indcollDatum, indclassDatum, indoptionDatum;
		bool		isnull;
		oidvector	*indcollation, *indclass; 
		int2vector	*indoption;
		Oid			indexrelid;
		HeapTuple       ht_idxrel, ht_am;
		Form_pg_class	idxrelrec;
		Form_pg_am      amrec;
		IndexAmRoutine 	*amroutine;
		Form_pg_index	index;
		List			*indexprs;
		int			keyno;
		char		*str;
		ListCell	*indexpr_item;
		List		*context;

		index = (Form_pg_index) GETSTRUCT(indexTuple);
		indexrelid = index->indexrelid;
		/* Must get indcollation, indclass, and indoption the hard way */
		indcollDatum = SysCacheGetAttr(INDEXRELID, indexTuple, 
						Anum_pg_index_indcollation, &isnull);
		Assert(!isnull);
		indcollation = (oidvector *) DatumGetPointer(indcollDatum);

		indclassDatum = SysCacheGetAttr(INDEXRELID, indexTuple, Anum_pg_index_indclass, &isnull);
		Assert(!isnull);
		indclass = (oidvector *) DatumGetPointer(indclassDatum);

		indoptionDatum = SysCacheGetAttr(INDEXRELID, indexTuple, Anum_pg_index_indoption, &isnull);
		Assert(!isnull);
		indoption = (int2vector *) DatumGetPointer(indoptionDatum);

		/*
 		 ** Fetch the pg_class tuple of the index relation
		 **/
		ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(indexrelid));
		if (!HeapTupleIsValid(ht_idxrel))
        		elog(ERROR, "cache lookup failed for relation %u", indexrelid);

		idxrelrec = (Form_pg_class) GETSTRUCT(ht_idxrel);
		
        	/*
 		 ** Fetch the pg_am tuple of the index' access method
		 */

		ht_am = SearchSysCache1(AMOID, ObjectIdGetDatum(idxrelrec->relam));
		if (!HeapTupleIsValid(ht_am))
                	elog(ERROR, "cache lookup failed for access method %u",
                        	 idxrelrec->relam);

		amrec = (Form_pg_am) GETSTRUCT(ht_am);

		/* Fetch the index AM's API struct */
		amroutine = GetIndexAmRoutine(amrec->amhandler);

		/*
 		 ** Get the index expressions, if any.  (NOTE: we do not use the relcache
		 ** versions of the expressions and predicate, because we want to display
 		 ** non-const-folded expressions.)
 		 **/
#if PG_VERSION_NUM >= 110000
		if (!heap_attisnull(indexTuple, Anum_pg_index_indexprs, NULL))
#else
		if (!heap_attisnull(indexTuple, Anum_pg_index_indexprs))
#endif
		{
			Datum           exprsDatum;
			bool            isnull;
			char       *exprsString;

			exprsDatum = SysCacheGetAttr(INDEXRELID, indexTuple, 
										 Anum_pg_index_indexprs, &isnull);
			Assert(!isnull);
			exprsString = TextDatumGetCString(exprsDatum);
			indexprs = (List *) stringToNode(exprsString);
			pfree(exprsString);
		}
		else
			indexprs = NIL;

        	indexpr_item = list_head(indexprs);

		context = deparse_context_for(get_relation_name(relid), relid);

		appendStringInfo(sql, "CREATE %sINDEX %s_%s ON %s.%s USING %s (",
				 index->indisunique ? "UNIQUE " : "",
				 remote_table, NameStr(idxrelrec->relname),
				 remote_schema, remote_table,
				 quote_identifier(NameStr(amrec->amname)));		
		
		for (keyno = 0; keyno < index->indnatts; keyno++) {
			AttrNumber  attnum = index->indkey.values[keyno];
			int16		opt = indoption->values[keyno];
			Oid			keycoltype;
			Oid			keycolcollation;

			if (keyno > 0)
				appendStringInfoString(sql, ", ");

			if (attnum != 0)
			{
				/* Simple index column */
				char	*attname;
				int32	keycoltypmod;
#if PG_VERSION_NUM >= 110000
				attname = get_attname(relid, attnum, false);
#else
				attname = get_relid_attribute_name(relid, attnum);
#endif
				appendStringInfoString(sql, quote_identifier(attname));
				get_atttypetypmodcoll(relid, attnum, &keycoltype, 
							&keycoltypmod, &keycolcollation);
			} else {
				/* expressional index */
				Node       *indexkey;

				if (indexpr_item == NULL)
					elog(ERROR, "too few entries in indexprs list");
				indexkey = (Node *) lfirst(indexpr_item);
				indexpr_item = lnext(indexpr_item);
				/* Deparse */
				str = deparse_expression(indexkey, context, false, false);
				/* Need parens if it's not a bare function call */
				if (looks_like_function(indexkey))
                			appendStringInfoString(sql, str);
				else
					appendStringInfo(sql, "(%s)", str);

				keycoltype = exprType(indexkey);
				keycolcollation = exprCollation(indexkey);
			}
			
			{
				Oid	indcoll;
				/* Add collation, if not default for column */
				indcoll = indcollation->values[keyno];
				if (OidIsValid(indcoll) && indcoll != keycolcollation)
					appendStringInfo(sql, " COLLATE %s", generate_collation_name((indcoll)));

				/* Add the operator class name, if not default */
				get_opclass_name(indclass->values[keyno], keycoltype, sql);

				/* Add options if relevant */
				if (amroutine->amcanorder)
				{
					/* if it supports sort ordering, report DESC and NULLS opts */
					if (opt & INDOPTION_DESC)
					{
						appendStringInfoString(sql, " DESC");
						/* NULLS FIRST is the default in this case */
						if (!(opt & INDOPTION_NULLS_FIRST))
							appendStringInfoString(sql, " NULLS LAST");
					}
					else
					{
						if (opt & INDOPTION_NULLS_FIRST)
							appendStringInfoString(sql, " NULLS FIRST");
					}
				}
			}
		}
		appendStringInfoChar(sql, ')');
		str = flatten_reloptions(indexrelid);
		if (str)
		{
			appendStringInfo(sql, " WITH (%s)", str);
			pfree(str);
		}

#if PG_VERSION_NUM >= 110000
		if (!heap_attisnull(indexTuple, Anum_pg_index_indpred, NULL))
#else
		if (!heap_attisnull(indexTuple, Anum_pg_index_indpred))
#endif
		{
			Node       *node;
			Datum           predDatum;
			bool            isnull;
			char       *predString;

			/* Convert text string to node tree */
			predDatum = SysCacheGetAttr(INDEXRELID, indexTuple, Anum_pg_index_indpred, &isnull);
			Assert(!isnull);
			predString = TextDatumGetCString(predDatum);
			node = (Node *) stringToNode(predString);
			pfree(predString);

			/* Deparse */
			str = deparse_expression(node, context, false, false);
			appendStringInfo(sql, " WHERE %s", str);
		}			
		appendStringInfoChar(sql, ';');
		ReleaseSysCache(ht_idxrel);
		ReleaseSysCache(ht_am);
	}

	heap_endscan(scan);

	relation_close(indRelation, AccessShareLock);
}

/*
 * sql need to be filled with the columns defintion 
 * */
void 
buildCTS4RemoteServer(StringInfoData* sql, Oid relid)
{

	MetaInfo *tbinfo =  NULL;
	int j = 0,  actual_atts = 0;

	tbinfo =  getTableAttrs(relid) ;

	for (; j < tbinfo->numatts; j++) {
		bool	has_default = (tbinfo->attrdefs[j] != NULL) ;
		bool	has_notnull = (tbinfo->notnull[j] && !tbinfo->inhNotNull[j]);
		if (actual_atts ==0) {
			appendStringInfo(sql, "(");	
		} else {
			appendStringInfo(sql, ",");
		}

		actual_atts++;
		appendStringInfo(sql, "\n    ");
		appendStringInfo(sql, " %s", tbinfo->attnames[j]);
		appendStringInfo(sql, " %s", tbinfo->atttypnames[j]);
		if (OidIsValid(tbinfo->attcollation[j])) {
			char *collname = NULL, *ns_name = NULL;
			getAttrsCollation(tbinfo->attcollation[j], &collname, &ns_name);
			appendStringInfo(sql, " COLLATE %s.%s", quote_literal_cstr(collname),
								quote_literal_cstr(ns_name));
		}

		if (has_default)
			appendStringInfo(sql, " DEFAULT %s", tbinfo->attrdefs[j]);

		if (has_notnull)
			appendStringInfo(sql, " NOT NULL");
	}
	appendStringInfo(sql, ")");

}

static void getAttrsCollation(Oid collationOid, char**collname, char**ns_name)
{
        Relation        rel;
        ScanKeyData scanKeyData;
        SysScanDesc scandesc;
        HeapTuple       tuple;
        rel = heap_open(CollationRelationId, AccessShareLock);

        ScanKeyInit(&scanKeyData,
                                ObjectIdAttributeNumber,
                                BTEqualStrategyNumber, F_OIDEQ,
                                ObjectIdGetDatum(collationOid));

        scandesc = systable_beginscan(rel, CollationOidIndexId, true,
                                                                  NULL, 1, &scanKeyData);

        tuple = systable_getnext(scandesc);

        if (HeapTupleIsValid(tuple)) {
		*collname = pstrdup(NameStr(((Form_pg_collation) GETSTRUCT(tuple))->collname));
		*ns_name =  get_namespace_name(((Form_pg_collation) GETSTRUCT(tuple))->collnamespace);
	}
        else
                elog(ERROR, "could not find tuple for collation %u", collationOid);

        systable_endscan(scandesc);

        heap_close(rel, AccessShareLock);
   	return ;
}
