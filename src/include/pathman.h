/* ------------------------------------------------------------------------
 *
 * pathman.h
 *		structures and prototypes for pathman functions
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PATHMAN_H
#define PATHMAN_H


#include "relation_info.h"
#include "rangeset.h"

#include "postgres.h"
#include "fmgr.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "nodes/execnodes.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "utils/timestamp.h"

/* Get CString representation of Datum (simple wrapper) */
#ifdef USE_ASSERT_CHECKING
	#include "utils.h"
	#define DebugPrintDatum(datum, typid) ( datum_to_cstring((datum), (typid)) )
#else
	#define DebugPrintDatum(datum, typid) ( "[use --enable-cassert]" )
#endif


#define PARTITION_TABLE_SCHEMA		"gogudb_partition_table"
/*
 * Main GUC variables.
 */
#define PATHMAN_ENABLE					"gogudb.enable"
#define PATHMAN_ENABLE_AUTO_PARTITION	"gogudb.enable_auto_partition"
#define PATHMAN_OVERRIDE_COPY			"gogudb.override_copy"


/*
 * Definitions for the "pathman_config" table.
 */
#define PATHMAN_CONFIG						"gogudb_config"
#define Natts_pathman_config				5
#define Anum_pathman_config_partrel			1	/* partitioned relation (regclass) */
#define Anum_pathman_config_expr			2	/* partition expression (original) */
#define Anum_pathman_config_parttype		3	/* partitioning type (1|2) */
#define Anum_pathman_config_range_interval	4	/* interval for RANGE pt. (text) */
#define Anum_pathman_config_cooked_expr		5	/* parsed partitioning expression (text) */

/*
 * Definitions for the "table_partition_rule" table.
 */
#define TABLE_PARTITION_RULE					"table_partition_rule"
#define Natts_table_partition_rule				9
#define Anum_table_partition_rule_schema			1	/* schema (text) */
#define Anum_table_partition_rule_relname			2	/* relation (text) */
#define Anum_table_partition_rule_cooked_expr		3	/*  partitioning expression (text) */
#define Anum_table_partition_rule_parttype		4	/* partitioning type (1|2) */
#define Anum_table_partition_rule_range_interval	5	/* interval for RANGE pt. (text) */
#define Anum_table_partition_rule_range_start		6	/* start for RANGE pt. (text) */
#define Anum_table_partition_rule_patitions_dist	7	/* number partttions on each server . (int) */
#define Anum_table_partition_rule_remote_schema		8	/* remote schema  (text) */
#define Anum_table_partition_rule_server_list		9	/* server list  (text) */

/*
 * Definitions for the "server_map" table.
 */
#define SERVER_MAP					"server_map"
#define Natts_server_map				3
#define Anum_server_map_srvname			1	/* server name (text) */
#define Anum_server_map_range_start			2	/* range start (int4) */
#define Anum_server_map_range_end			3	/* range end (int4) */


/* type modifier (typmod) for 'range_interval' */
#define PATHMAN_CONFIG_interval_typmod		-1

/*
 * Definitions for the "pathman_config_params" table.
 */
#define PATHMAN_CONFIG_PARAMS						"gogudb_config_params"
#define Natts_pathman_config_params					5
#define Anum_pathman_config_params_partrel			1	/* primary key */
#define Anum_pathman_config_params_enable_parent	2	/* include parent into plan */
#define Anum_pathman_config_params_auto				3	/* auto partitions creation */
#define Anum_pathman_config_params_init_callback	4	/* partition action callback */
#define Anum_pathman_config_params_spawn_using_bgw	5	/* should we use spawn BGW? */

/*
 * Definitions for the "pathman_partition_list" view.
 */
#define PATHMAN_PARTITION_LIST				"gogudb_partition_list"
#define Natts_pathman_partition_list		6
#define Anum_pathman_pl_parent				1	/* partitioned relation (regclass) */
#define Anum_pathman_pl_partition			2	/* child partition (regclass) */
#define Anum_pathman_pl_parttype			3	/* partitioning type (1|2) */
#define Anum_pathman_pl_partattr			4	/* partitioned column (text) */
#define Anum_pathman_pl_range_min			5	/* partition's min value */
#define Anum_pathman_pl_range_max			6	/* partition's max value */

/*
 * Definitions for the "pathman_cache_stats" view.
 */
#define PATHMAN_CACHE_STATS					"gogudb_cache_stats"
#define Natts_pathman_cache_stats			4
#define Anum_pathman_cs_context				1	/* name of memory context */
#define Anum_pathman_cs_size				2	/* size of memory context */
#define Anum_pathman_cs_used				3	/* used space */
#define Anum_pathman_cs_entries				4	/* number of cache entries */


/*
 * Cache current PATHMAN_CONFIG relid (set during load_config()).
 */
extern Oid	pathman_config_relid;
extern Oid	pathman_config_params_relid;
extern Oid	table_partition_rule_relid;
extern Oid	server_map_relid;
/*
 * Just to clarify our intentions (return the corresponding relid).
 */
Oid get_pathman_config_relid(bool invalid_is_ok);
Oid get_pathman_config_params_relid(bool invalid_is_ok);
Oid get_table_partition_rule_relid(bool invalid_is_ok);
Oid get_server_map_relid(bool invalid_is_ok);

/*
 * Create RelOptInfo & RTE for a selected partition.
 */
Index append_child_relation(PlannerInfo *root,
							Relation parent_relation,
							PlanRowMark *parent_rowmark,
							Index parent_rti,
							int ir_index,
							Oid child_oid,
							List *wrappers);


/*
 * Copied from PostgreSQL (prepunion.c)
 */
void make_inh_translation_list(Relation oldrelation, Relation newrelation,
							   Index newvarno, List **translated_vars);

Bitmapset *translate_col_privs(const Bitmapset *parent_privs,
							   List *translated_vars);


/*
 * Copied from PostgreSQL (allpaths.c)
 */
void set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti,
							 PathKey *pathkeyAsc, PathKey *pathkeyDesc);

Path *get_cheapest_parameterized_child_path(PlannerInfo *root, RelOptInfo *rel,
											Relids required_outer);


typedef struct
{
	const Node			   *orig;		/* examined expression */
	List				   *args;		/* clauses/wrappers extracted from 'orig' */
	List				   *rangeset;	/* IndexRanges representing selected parts */
	double					paramsel;	/* estimated selectivity of PARAMs
										   (for RuntimeAppend costs) */
	bool					found_gap;	/* were there any gaps? */
} WrapperNode;

#define InvalidWrapperNode	{ NULL, NIL, NIL, 0.0, false }

typedef struct
{
	Node				   *prel_expr;		/* expression from PartRelationInfo */
	const PartRelationInfo *prel;			/* main partitioning structure */
	ExprContext			   *econtext;		/* for ExecEvalExpr() */
} WalkerContext;

/* Usual initialization procedure for WalkerContext */
#define InitWalkerContext(context, expr, prel_info, ecxt) \
	do { \
		(context)->prel_expr = (expr); \
		(context)->prel = (prel_info); \
		(context)->econtext = (ecxt); \
	} while (0)

/* Check that WalkerContext contains ExprContext (plan execution stage) */
#define WcxtHasExprContext(wcxt) ( (wcxt)->econtext != NULL )

/* Examine expression in order to select partitions */
WrapperNode *walk_expr_tree(Expr *expr, const WalkerContext *context);


void select_range_partitions(const Datum value,
							 const Oid collid,
							 FmgrInfo *cmp_func,
							 const RangeEntry *ranges,
							 const int nranges,
							 const int strategy,
							 WrapperNode *result);


/* Convert hash value to the partition index */
static inline uint32
hash_to_part_index(uint32 value, uint32 partitions)
{
	return value % partitions;
}


/*
 * Compare two Datums using the given comarison function.
 *
 * flinfo is a pointer to FmgrInfo, arg1 & arg2 are Datums.
 */
#define check_lt(finfo, collid, arg1, arg2) \
	( DatumGetInt32(FunctionCall2Coll((finfo), (collid), (arg1), (arg2))) < 0 )

#define check_le(finfo, collid, arg1, arg2) \
	( DatumGetInt32(FunctionCall2Coll((finfo), (collid), (arg1), (arg2))) <= 0 )

#define check_eq(finfo, collid, arg1, arg2) \
	( DatumGetInt32(FunctionCall2Coll((finfo), (collid), (arg1), (arg2))) == 0 )

#define check_ge(finfo, collid, arg1, arg2) \
	( DatumGetInt32(FunctionCall2Coll((finfo), (collid), (arg1), (arg2))) >= 0 )

#define check_gt(finfo, collid, arg1, arg2) \
	( DatumGetInt32(FunctionCall2Coll((finfo), (collid), (arg1), (arg2))) > 0 )


#endif /* PATHMAN_H */
