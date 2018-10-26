/* ------------------------------------------------------------------------
 *
 * partition_filter.h
 *		Select partition for INSERT operation
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PARTITION_FILTER_H
#define PARTITION_FILTER_H


#include "relation_info.h"
#include "utils.h"

#include "postgres.h"
#include "access/tupconvert.h"
#include "commands/explain.h"
#include "optimizer/planner.h"

#if PG_VERSION_NUM >= 90600
#include "nodes/extensible.h"
#endif


#define ERR_PART_ATTR_NULL				"partitioning expression's value should not be NULL"
#define ERR_PART_ATTR_MULTIPLE_RESULTS	"partitioning expression should return single value"
#define ERR_PART_ATTR_NO_PART			"no suitable partition for key '%s'"
#define ERR_PART_ATTR_MULTIPLE			"PartitionFilter selected more than one partition"
#define ERR_PART_DESC_CONVERT			"could not convert row type for partition"


/*
 * Single element of 'result_rels_table'.
 */
typedef struct
{
	Oid					partid;				/* partition's relid */
	ResultRelInfo	   *result_rel_info;	/* cached ResultRelInfo */
	TupleConversionMap *tuple_map;			/* tuple conversion map (parent => child) */
} ResultRelInfoHolder;


/* Forward declaration (for on_rri_holder()) */
struct ResultPartsStorage;
typedef struct ResultPartsStorage ResultPartsStorage;

/*
 * Callback to be fired at rri_holder creation/destruction.
 */
typedef void (*on_rri_holder)(ResultRelInfoHolder *rri_holder,
							  const ResultPartsStorage *rps_storage);

/*
 * Cached ResultRelInfos of partitions.
 */
struct ResultPartsStorage
{
	ResultRelInfo	   *saved_rel_info;			/* original ResultRelInfo (parent) */
	HTAB			   *result_rels_table;
	HASHCTL				result_rels_table_config;

	bool				speculative_inserts;	/* for ExecOpenIndices() */

	on_rri_holder		on_new_rri_holder_callback;
	void			   *callback_arg;

	EState			   *estate;					/* pointer to executor's state */

	CmdType				command_type;			/* currently we only allow INSERT */
	LOCKMODE			head_open_lock_mode;
	LOCKMODE			heap_close_lock_mode;
};

/*
 * Standard size of ResultPartsStorage entry.
 */
#define ResultPartsStorageStandard	0

typedef struct
{
	CustomScanState		css;

	Oid					partitioned_table;
	OnConflictAction	on_conflict_action;
	List			   *returning_list;

	Plan			   *subplan;				/* proxy variable to store subplan */
	ResultPartsStorage	result_parts;			/* partition ResultRelInfo cache */

	bool				warning_triggered;		/* warning message counter */

	TupleTableSlot	   *tup_convert_slot;		/* slot for rebuilt tuples */
	ExprContext		   *tup_convert_econtext;	/* ExprContext for projections */

	ExprState		   *expr_state;				/* for partitioning expression */
} PartitionFilterState;


extern bool					pg_pathman_enable_partition_filter;
extern int					pg_pathman_insert_into_fdw;

extern CustomScanMethods	partition_filter_plan_methods;
extern CustomExecMethods	partition_filter_exec_methods;


void init_partition_filter_static_data(void);


/* ResultPartsStorage init\fini\scan function */
void init_result_parts_storage(ResultPartsStorage *parts_storage,
							   EState *estate,
							   bool speculative_inserts,
							   Size table_entry_size,
							   on_rri_holder on_new_rri_holder_cb,
							   void *on_new_rri_holder_cb_arg);

void fini_result_parts_storage(ResultPartsStorage *parts_storage,
							   bool close_rels, on_rri_holder hook);

ResultRelInfoHolder * scan_result_parts_storage(Oid partid,
												ResultPartsStorage *storage);

TupleConversionMap * build_part_tuple_map(Relation parent_rel, Relation child_rel);


/* Find suitable partition using 'value' */
Oid * find_partitions_for_value(Datum value, Oid value_type,
								const PartRelationInfo *prel,
								int *nparts);

ResultRelInfoHolder * select_partition_for_insert(Datum value, Oid value_type,
												  const PartRelationInfo *prel,
												  ResultPartsStorage *parts_storage,
												  EState *estate);


Plan * make_partition_filter(Plan *subplan,
							 Oid parent_relid,
							 Index parent_rti,
							 OnConflictAction conflict_action,
							 List *returning_list);


Node * partition_filter_create_scan_state(CustomScan *node);

void partition_filter_begin(CustomScanState *node,
							EState *estate,
							int eflags);

TupleTableSlot * partition_filter_exec(CustomScanState *node);

void partition_filter_end(CustomScanState *node);

void partition_filter_rescan(CustomScanState *node);

void partition_filter_explain(CustomScanState *node,
							  List *ancestors,
							  ExplainState *es);


#endif /* PARTITION_FILTER_H */
