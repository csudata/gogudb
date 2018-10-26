/* ------------------------------------------------------------------------
 *
 * partition_filter.c
 *		Select partition for INSERT operation
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "compat/pg_compat.h"
#include "init.h"
#include "nodes_common.h"
#include "pathman.h"
#include "partition_creation.h"
#include "partition_filter.h"
#include "utils.h"

#include "catalog/pg_type.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/nodeFuncs.h"
#include "rewrite/rewriteManip.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


#define ALLOC_EXP	2


/*
 * HACK: 'estate->es_query_cxt' as data storage
 *
 * We use this struct as an argument for fake
 * MemoryContextCallback pf_memcxt_callback()
 * in order to attach some additional info to
 * EState (estate->es_query_cxt is involved).
 */
typedef struct
{
	int		estate_alloc_result_rels;	/* number of allocated result rels */
	bool	estate_not_modified;		/* did we modify EState somehow? */
} estate_mod_data;

/*
 * Allow INSERTs into any FDW \ postgres_fdw \ no FDWs at all.
 */
typedef enum
{
	PF_FDW_INSERT_DISABLED = 0,		/* INSERTs into FDWs are prohibited */
	PF_FDW_INSERT_POSTGRES,			/* INSERTs into postgres_fdw are OK */
	PF_FDW_INSERT_ANY_FDW			/* INSERTs into any FDWs are OK */
} PF_insert_fdw_mode;

static const struct config_enum_entry pg_pathman_insert_into_fdw_options[] = {
	{ "disabled",	PF_FDW_INSERT_DISABLED,	false },
	{ "postgres",	PF_FDW_INSERT_POSTGRES,	false },
	{ "any_fdw",	PF_FDW_INSERT_ANY_FDW,	false },
	{ NULL,			0,						false }
};


bool				pg_pathman_enable_partition_filter = true;
int					pg_pathman_insert_into_fdw = PF_FDW_INSERT_POSTGRES;

CustomScanMethods	partition_filter_plan_methods;
CustomExecMethods	partition_filter_exec_methods;


static void prepare_rri_for_insert(ResultRelInfoHolder *rri_holder,
								   const ResultPartsStorage *rps_storage);
static void prepare_rri_returning_for_insert(ResultRelInfoHolder *rri_holder,
											 const ResultPartsStorage *rps_storage);
static void prepare_rri_fdw_for_insert(ResultRelInfoHolder *rri_holder,
									   const ResultPartsStorage *rps_storage);
static Node *fix_returning_list_mutator(Node *node, void *state);

static Index append_rte_to_estate(EState *estate, RangeTblEntry *rte);
static int append_rri_to_estate(EState *estate, ResultRelInfo *rri);

static List * pfilter_build_tlist(Relation parent_rel, List *tlist);

static void pf_memcxt_callback(void *arg);
static estate_mod_data * fetch_estate_mod_data(EState *estate);


void
init_partition_filter_static_data(void)
{
	partition_filter_plan_methods.CustomName 			= "PartitionFilter";
	partition_filter_plan_methods.CreateCustomScanState	= partition_filter_create_scan_state;

	partition_filter_exec_methods.CustomName			= "PartitionFilter";
	partition_filter_exec_methods.BeginCustomScan		= partition_filter_begin;
	partition_filter_exec_methods.ExecCustomScan		= partition_filter_exec;
	partition_filter_exec_methods.EndCustomScan			= partition_filter_end;
	partition_filter_exec_methods.ReScanCustomScan		= partition_filter_rescan;
	partition_filter_exec_methods.MarkPosCustomScan		= NULL;
	partition_filter_exec_methods.RestrPosCustomScan	= NULL;
	partition_filter_exec_methods.ExplainCustomScan		= partition_filter_explain;

	
	DefineCustomBoolVariable("gogudb.enable_partitionfilter",
							 "Enables the planner's use of PartitionFilter custom node.",
							 NULL,
							 &pg_pathman_enable_partition_filter,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	/*
	DefineCustomEnumVariable("pg_pathman.insert_into_fdw",
							 "Allow INSERTS into FDW partitions.",
							 NULL,
							 &pg_pathman_insert_into_fdw,
							 PF_FDW_INSERT_POSTGRES,
							 pg_pathman_insert_into_fdw_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);*/
}


/*
 * ---------------------------
 *  Partition Storage (cache)
 * ---------------------------
 */

/* Initialize ResultPartsStorage (hash table etc) */
void
init_result_parts_storage(ResultPartsStorage *parts_storage,
						  EState *estate,
						  bool speculative_inserts,
						  Size table_entry_size,
						  on_rri_holder on_new_rri_holder_cb,
						  void *on_new_rri_holder_cb_arg)
{
	HASHCTL *result_rels_table_config = &parts_storage->result_rels_table_config;

	memset(result_rels_table_config, 0, sizeof(HASHCTL));
	result_rels_table_config->keysize = sizeof(Oid);

	/* Use sizeof(ResultRelInfoHolder) if table_entry_size is 0 */
	if (table_entry_size == ResultPartsStorageStandard)
		result_rels_table_config->entrysize = sizeof(ResultRelInfoHolder);
	else
		result_rels_table_config->entrysize = table_entry_size;

	parts_storage->result_rels_table = hash_create("ResultRelInfo storage", 10,
												   result_rels_table_config,
												   HASH_ELEM | HASH_BLOBS);
	parts_storage->estate = estate;
	parts_storage->saved_rel_info = NULL;

	parts_storage->on_new_rri_holder_callback = on_new_rri_holder_cb;
	parts_storage->callback_arg = on_new_rri_holder_cb_arg;

	/* Currently ResultPartsStorage is used only for INSERTs */
	parts_storage->command_type = CMD_INSERT;
	parts_storage->speculative_inserts = speculative_inserts;

	/* Partitions must remain locked till transaction's end */
	parts_storage->head_open_lock_mode = RowExclusiveLock;
	parts_storage->heap_close_lock_mode = NoLock;
}

/* Free ResultPartsStorage (close relations etc) */
void
fini_result_parts_storage(ResultPartsStorage *parts_storage, bool close_rels,
						  on_rri_holder hook)
{
	HASH_SEQ_STATUS			stat;
	ResultRelInfoHolder	   *rri_holder; /* ResultRelInfo holder */

	hash_seq_init(&stat, parts_storage->result_rels_table);
	while ((rri_holder = (ResultRelInfoHolder *) hash_seq_search(&stat)) != NULL)
	{
		/* Call destruction hook, if needed */
		if (hook != NULL)
			hook(rri_holder, parts_storage);

		/* Close partitions and free free conversion-related stuff */
		if (close_rels)
		{
			ExecCloseIndices(rri_holder->result_rel_info);

			heap_close(rri_holder->result_rel_info->ri_RelationDesc,
					   parts_storage->heap_close_lock_mode);

			/* Skip if there's no map */
			if (!rri_holder->tuple_map)
				continue;

			FreeTupleDesc(rri_holder->tuple_map->indesc);
			FreeTupleDesc(rri_holder->tuple_map->outdesc);

			free_conversion_map(rri_holder->tuple_map);
		}
		/* Else just free conversion-related stuff */
		else
		{
			/* Skip if there's no map */
			if (!rri_holder->tuple_map)
				continue;

			FreeTupleDesc(rri_holder->tuple_map->indesc);
			FreeTupleDesc(rri_holder->tuple_map->outdesc);

			free_conversion_map(rri_holder->tuple_map);
		}
	}

	/* Finally destroy hash table */
	hash_destroy(parts_storage->result_rels_table);
}

/* Find a ResultRelInfo for the partition using ResultPartsStorage */
ResultRelInfoHolder *
scan_result_parts_storage(Oid partid, ResultPartsStorage *parts_storage)
{
#define CopyToResultRelInfo(field_name) \
	( child_result_rel_info->field_name = parts_storage->saved_rel_info->field_name )

	ResultRelInfoHolder	   *rri_holder;
	bool					found;

	rri_holder = hash_search(parts_storage->result_rels_table,
							 (const void *) &partid,
							 HASH_ENTER, &found);

	/* If not found, create & cache new ResultRelInfo */
	if (!found)
	{
		Relation		child_rel,
						parent_rel = parts_storage->saved_rel_info->ri_RelationDesc;
		RangeTblEntry  *child_rte,
					   *parent_rte;
		Index			child_rte_idx;
		ResultRelInfo  *child_result_rel_info;
		List		   *translated_vars;

		/* Lock partition and check if it exists */
		LockRelationOid(partid, parts_storage->head_open_lock_mode);
		if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(partid)))
		{
			/* Don't forget to drop invalid hash table entry */
			hash_search(parts_storage->result_rels_table,
						(const void *) &partid,
						HASH_REMOVE, NULL);

			UnlockRelationOid(partid, parts_storage->head_open_lock_mode);
			return NULL;
		}

		parent_rte = rt_fetch(parts_storage->saved_rel_info->ri_RangeTableIndex,
							  parts_storage->estate->es_range_table);

		/* Open relation and check if it is a valid target */
		child_rel = heap_open(partid, NoLock);

		/* Build Var translation list for 'inserted_cols' */
		make_inh_translation_list(parent_rel, child_rel, 0, &translated_vars);

		/* Create RangeTblEntry for partition */
		child_rte = makeNode(RangeTblEntry);
		child_rte->rtekind			= RTE_RELATION;
		child_rte->relid			= partid;
		child_rte->relkind			= child_rel->rd_rel->relkind;
		child_rte->eref				= parent_rte->eref;
		child_rte->requiredPerms	= parent_rte->requiredPerms;
		child_rte->checkAsUser		= parent_rte->checkAsUser;
		child_rte->insertedCols		= translate_col_privs(parent_rte->insertedCols,
														  translated_vars);
		child_rte->updatedCols		= translate_col_privs(parent_rte->updatedCols,
														  translated_vars);

		/* Check permissions for partition */
		ExecCheckRTPerms(list_make1(child_rte), true);

		/* Append RangeTblEntry to estate->es_range_table */
		child_rte_idx = append_rte_to_estate(parts_storage->estate, child_rte);

		/* Create ResultRelInfo for partition */
		child_result_rel_info = makeNode(ResultRelInfo);

		/* Check that 'saved_rel_info' is set */
		if (!parts_storage->saved_rel_info)
			elog(ERROR, "ResultPartsStorage contains no saved_rel_info");

		InitResultRelInfoCompat(child_result_rel_info,
								child_rel,
								child_rte_idx,
								parts_storage->estate->es_instrument);

		if (parts_storage->command_type != CMD_DELETE)
			ExecOpenIndices(child_result_rel_info, parts_storage->speculative_inserts);

		/* Copy necessary fields from saved ResultRelInfo */
		CopyToResultRelInfo(ri_WithCheckOptions);
		CopyToResultRelInfo(ri_WithCheckOptionExprs);
		CopyToResultRelInfo(ri_junkFilter);
		CopyToResultRelInfo(ri_projectReturning);
#if PG_VERSION_NUM >= 110000
		CopyToResultRelInfo(ri_onConflictArbiterIndexes);
		CopyToResultRelInfo(ri_onConflict);
#else
		CopyToResultRelInfo(ri_onConflictSetProj);
		CopyToResultRelInfo(ri_onConflictSetWhere);
#endif
		/* ri_ConstraintExprs will be initialized by ExecRelCheck() */
		child_result_rel_info->ri_ConstraintExprs = NULL;

		/* Check that this partition is a valid result relation */
		CheckValidResultRelCompat(child_result_rel_info, parts_storage->command_type);

		/* Fill the ResultRelInfo holder */
		rri_holder->partid = partid;
		rri_holder->result_rel_info = child_result_rel_info;

		/* Generate tuple transformation map and some other stuff */
		rri_holder->tuple_map = build_part_tuple_map(parent_rel, child_rel);

		/* Call on_new_rri_holder_callback() if needed */
		if (parts_storage->on_new_rri_holder_callback)
			parts_storage->on_new_rri_holder_callback(rri_holder,
													  parts_storage);

		/* Finally append ResultRelInfo to storage->es_alloc_result_rels */
		append_rri_to_estate(parts_storage->estate, child_result_rel_info);
	}

	return rri_holder;
}


/* Build tuple conversion map (e.g. parent has a dropped column) */
TupleConversionMap *
build_part_tuple_map(Relation parent_rel, Relation child_rel)
{
	TupleConversionMap *tuple_map;
	TupleDesc			child_tupdesc,
						parent_tupdesc;

	/* HACK: use fake 'tdtypeid' in order to fool convert_tuples_by_name() */
	child_tupdesc = CreateTupleDescCopy(RelationGetDescr(child_rel));
	child_tupdesc->tdtypeid = InvalidOid;

	parent_tupdesc = CreateTupleDescCopy(RelationGetDescr(parent_rel));
	parent_tupdesc->tdtypeid = InvalidOid;

	/* Generate tuple transformation map and some other stuff */
	tuple_map = convert_tuples_by_name(parent_tupdesc,
									   child_tupdesc,
									   ERR_PART_DESC_CONVERT);

	/* If map is one-to-one, free unused TupleDescs */
	if (!tuple_map)
	{
		FreeTupleDesc(child_tupdesc);
		FreeTupleDesc(parent_tupdesc);
	}

	return tuple_map;
}


/*
 * -----------------------------------
 *  Partition search helper functions
 * -----------------------------------
 */

/*
 * Find matching partitions for 'value' using PartRelationInfo.
 */
Oid *
find_partitions_for_value(Datum value, Oid value_type,
						  const PartRelationInfo *prel,
						  int *nparts)
{
#define CopyToTempConst(const_field, attr_field) \
	( temp_const.const_field = prel->attr_field )

	Const			temp_const;	/* temporary const for expr walker */
	WalkerContext	wcxt;
	List		   *ranges = NIL;

	/* Prepare dummy Const node */
	NodeSetTag(&temp_const, T_Const);
	temp_const.location = -1;

	/* Fill const with value ... */
	temp_const.constvalue	= value;
	temp_const.consttype	= value_type;
	temp_const.constisnull	= false;

	/* ... and some other important data */
	CopyToTempConst(consttypmod, ev_typmod);
	CopyToTempConst(constcollid, ev_collid);
	CopyToTempConst(constlen,    ev_len);
	CopyToTempConst(constbyval,  ev_byval);

	/* We use 0 since varno doesn't matter for Const */
	InitWalkerContext(&wcxt, 0, prel, NULL);
	ranges = walk_expr_tree((Expr *) &temp_const, &wcxt)->rangeset;

	return get_partition_oids(ranges, nparts, prel, false);
}

/*
 * Smart wrapper for scan_result_parts_storage().
 */
ResultRelInfoHolder *
select_partition_for_insert(Datum value, Oid value_type,
							const PartRelationInfo *prel,
							ResultPartsStorage *parts_storage,
							EState *estate)
{
	MemoryContext			old_mcxt;
	ResultRelInfoHolder	   *rri_holder;
	Oid						parent_relid = PrelParentRelid(prel);
	Oid						selected_partid = InvalidOid;
	Oid					   *parts;
	int						nparts;

	do
	{
		/* Search for matching partitions */
		parts = find_partitions_for_value(value, value_type, prel, &nparts);

		if (nparts > 1)
			elog(ERROR, ERR_PART_ATTR_MULTIPLE);
		else if (nparts == 0)
		{
			 selected_partid = create_partitions_for_value(parent_relid,
														   value, value_type);

			 /* get_pathman_relation_info() will refresh this entry */
			 invalidate_pathman_relation_info(parent_relid, NULL);
		}
		else selected_partid = parts[0];

		/* Replace parent table with a suitable partition */
		old_mcxt = MemoryContextSwitchTo(estate->es_query_cxt);
		rri_holder = scan_result_parts_storage(selected_partid, parts_storage);
		MemoryContextSwitchTo(old_mcxt);

		/* This partition has been dropped, repeat with a new 'prel' */
		if (rri_holder == NULL)
		{
			/* get_pathman_relation_info() will refresh this entry */
			invalidate_pathman_relation_info(parent_relid, NULL);

			/* Get a fresh PartRelationInfo */
			prel = get_pathman_relation_info(parent_relid);

			/* Paranoid check (all partitions have vanished) */
			if (!prel)
				elog(ERROR, "table \"%s\" is not partitioned",
					 get_rel_name_or_relid(parent_relid));
		}
	}
	/* Loop until we get some result */
	while (rri_holder == NULL);

	return rri_holder;
}


/*
 * --------------------------------
 *  PartitionFilter implementation
 * --------------------------------
 */

Plan *
make_partition_filter(Plan *subplan,
					  Oid parent_relid,
					  Index parent_rti,
					  OnConflictAction conflict_action,
					  List *returning_list)
{
	CustomScan *cscan = makeNode(CustomScan);
	Relation	parent_rel;

	/* Currently we don't support ON CONFLICT clauses */
	if (conflict_action != ONCONFLICT_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ON CONFLICT clause is not supported with partitioned tables")));

	/* Copy costs etc */
	cscan->scan.plan.startup_cost = subplan->startup_cost;
	cscan->scan.plan.total_cost = subplan->total_cost;
	cscan->scan.plan.plan_rows = subplan->plan_rows;
	cscan->scan.plan.plan_width = subplan->plan_width;

	/* Setup methods and child plan */
	cscan->methods = &partition_filter_plan_methods;
	cscan->custom_plans = list_make1(subplan);

	/* Build an appropriate target list using a cached Relation entry */
	parent_rel = RelationIdGetRelation(parent_relid);
	cscan->scan.plan.targetlist = pfilter_build_tlist(parent_rel, subplan->targetlist);
	RelationClose(parent_rel);

	/* No physical relation will be scanned */
	cscan->scan.scanrelid = 0;

	/* Prepare 'custom_scan_tlist' for EXPLAIN (VERBOSE) */
	cscan->custom_scan_tlist = copyObject(cscan->scan.plan.targetlist);
	ChangeVarNodes((Node *) cscan->custom_scan_tlist, INDEX_VAR, parent_rti, 0);

	/* Pack partitioned table's Oid and conflict_action */
	cscan->custom_private = list_make3(makeInteger(parent_relid),
									   makeInteger(conflict_action),
									   returning_list);

	return &cscan->scan.plan;
}

Node *
partition_filter_create_scan_state(CustomScan *node)
{
	PartitionFilterState *state;

	state = (PartitionFilterState *) palloc0(sizeof(PartitionFilterState));
	NodeSetTag(state, T_CustomScanState);

	state->css.flags = node->flags;
	state->css.methods = &partition_filter_exec_methods;

	/* Extract necessary variables */
	state->subplan = (Plan *) linitial(node->custom_plans);
	state->partitioned_table = intVal(linitial(node->custom_private));
	state->on_conflict_action = intVal(lsecond(node->custom_private));
	state->returning_list = lthird(node->custom_private);

	/* Check boundaries */
	Assert(state->on_conflict_action >= ONCONFLICT_NONE ||
		   state->on_conflict_action <= ONCONFLICT_UPDATE);

	state->expr_state = NULL;

	/* There should be exactly one subplan */
	Assert(list_length(node->custom_plans) == 1);

	return (Node *) state;
}

void
partition_filter_begin(CustomScanState *node, EState *estate, int eflags)
{
	PartitionFilterState	   *state = (PartitionFilterState *) node;

	MemoryContext				old_mcxt;
	const PartRelationInfo	   *prel;
	Node					   *expr;
	Index						parent_varno = 1;
	ListCell				   *lc;

	/* It's convenient to store PlanState in 'custom_ps' */
	node->custom_ps = list_make1(ExecInitNode(state->subplan, estate, eflags));

	if (state->expr_state == NULL)
	{
		/* Fetch PartRelationInfo for this partitioned relation */
		prel = get_pathman_relation_info(state->partitioned_table);
		Assert(prel != NULL);

		/* Change varno in Vars according to range table */
		foreach(lc, estate->es_range_table)
		{
			RangeTblEntry *entry = lfirst(lc);

			if (entry->relid == state->partitioned_table)
				break;

			parent_varno += 1;
		}
		expr = PrelExpressionForRelid(prel, parent_varno);

		/* Prepare state for expression execution */
		old_mcxt = MemoryContextSwitchTo(estate->es_query_cxt);
		state->expr_state = ExecInitExpr((Expr *) expr, NULL);
		MemoryContextSwitchTo(old_mcxt);
	}

	/* Init ResultRelInfo cache */
	init_result_parts_storage(&state->result_parts, estate,
							  state->on_conflict_action != ONCONFLICT_NONE,
							  ResultPartsStorageStandard,
							  prepare_rri_for_insert,
							  (void *) state);

	state->warning_triggered = false;
}

TupleTableSlot *
partition_filter_exec(CustomScanState *node)
{
	PartitionFilterState   *state = (PartitionFilterState *) node;

	ExprContext			   *econtext = node->ss.ps.ps_ExprContext;
	EState				   *estate = node->ss.ps.state;
	PlanState			   *child_ps = (PlanState *) linitial(node->custom_ps);
	TupleTableSlot		   *slot;

	slot = ExecProcNode(child_ps);

	/* Save original ResultRelInfo */
	if (!state->result_parts.saved_rel_info)
		state->result_parts.saved_rel_info = estate->es_result_relation_info;

	if (!TupIsNull(slot))
	{
		MemoryContext				old_mcxt;
		const PartRelationInfo	   *prel;
		ResultRelInfoHolder		   *rri_holder;
		bool						isnull;
		Datum						value;
		TupleTableSlot			   *tmp_slot;

		/* Fetch PartRelationInfo for this partitioned relation */
		prel = get_pathman_relation_info(state->partitioned_table);
		if (!prel)
		{
			if (!state->warning_triggered)
				elog(WARNING, "table \"%s\" is not partitioned, "
							  "PartitionFilter will behave as a normal INSERT",
					 get_rel_name_or_relid(state->partitioned_table));

			return slot;
		}

		/* Switch to per-tuple context */
		old_mcxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		/* Execute expression */
		tmp_slot = econtext->ecxt_scantuple;
		econtext->ecxt_scantuple = slot;
		value = ExecEvalExprCompat(state->expr_state, econtext, &isnull,
								   mult_result_handler);
		econtext->ecxt_scantuple = tmp_slot;

		if (isnull)
			elog(ERROR, ERR_PART_ATTR_NULL);

		/*
		 * Search for a matching partition.
		 * WARNING: 'prel' might change after this call!
		 */
		rri_holder = select_partition_for_insert(value, prel->ev_type, prel,
												 &state->result_parts, estate);

		/* Switch back and clean up per-tuple context */
		MemoryContextSwitchTo(old_mcxt);
		ResetExprContext(econtext);

		/* Magic: replace parent's ResultRelInfo with ours */
		estate->es_result_relation_info = rri_holder->result_rel_info;

		/* If there's a transform map, rebuild the tuple */
		if (rri_holder->tuple_map)
		{
			HeapTuple	htup_old,
						htup_new;
			Relation	child_rel = rri_holder->result_rel_info->ri_RelationDesc;

			htup_old = ExecMaterializeSlot(slot);
			htup_new = do_convert_tuple(htup_old, rri_holder->tuple_map);

			/* Allocate new slot if needed */
			if (!state->tup_convert_slot)
#if PG_VERSION_NUM >= 110000
				state->tup_convert_slot = MakeTupleTableSlot(NULL);
#else
				state->tup_convert_slot = MakeTupleTableSlot();
#endif
			ExecSetSlotDescriptor(state->tup_convert_slot, RelationGetDescr(child_rel));
			ExecStoreTuple(htup_new, state->tup_convert_slot, InvalidBuffer, true);

			/* Now replace the original slot */
			slot = state->tup_convert_slot;
		}

		return slot;
	}

	return NULL;
}

void
partition_filter_end(CustomScanState *node)
{
	PartitionFilterState   *state = (PartitionFilterState *) node;

	/* Executor will close rels via estate->es_result_relations */
	fini_result_parts_storage(&state->result_parts, false, NULL);

	Assert(list_length(node->custom_ps) == 1);
	ExecEndNode((PlanState *) linitial(node->custom_ps));

	/* Free slot for tuple conversion */
	if (state->tup_convert_slot)
		ExecDropSingleTupleTableSlot(state->tup_convert_slot);
}

void
partition_filter_rescan(CustomScanState *node)
{
	Assert(list_length(node->custom_ps) == 1);
	ExecReScan((PlanState *) linitial(node->custom_ps));
}

void
partition_filter_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	/* Nothing to do here now */
}



/*
 * Build partition filter's target list pointing to subplan tuple's elements.
 */
static List *
pfilter_build_tlist(Relation parent_rel, List *tlist)
{
	List	   *result_tlist = NIL;
	ListCell   *lc;
	int			i = 1;

	foreach (lc, tlist)
	{
		TargetEntry		   *tle = (TargetEntry *) lfirst(lc);
		Expr			   *col_expr;
		Form_pg_attribute	attr;

		/* Make sure that this attribute exists */
		if (i > RelationGetDescr(parent_rel)->natts)
			elog(ERROR, "error in function " CppAsString(pfilter_build_tlist));

		/* Fetch pg_attribute entry for this column */
#if PG_VERSION_NUM >= 110000
		attr = &(RelationGetDescr(parent_rel)->attrs[i - 1]);
#else
		attr = RelationGetDescr(parent_rel)->attrs[i - 1];
#endif
		/* If this column is dropped, create a placeholder Const */
		if (attr->attisdropped)
		{
			/* Insert NULL for dropped column */
			col_expr = (Expr *) makeConst(INT4OID,
										  -1,
										  InvalidOid,
										  sizeof(int32),
										  (Datum) 0,
										  true,
										  true);
		}
		/* Otherwise we should create a Var referencing subplan's output */
		else
		{
			col_expr = (Expr *) makeVar(INDEX_VAR,	/* point to subplan's elements */
										i,			/* direct attribute mapping */
										exprType((Node *) tle->expr),
										exprTypmod((Node *) tle->expr),
										exprCollation((Node *) tle->expr),
										0);
		}

		result_tlist = lappend(result_tlist,
							   makeTargetEntry(col_expr,
											   i,
											   NULL,
											   tle->resjunk));
		i++; /* next resno */
	}

	return result_tlist;
}


/*
 * ----------------------------------------------
 *  Additional init steps for ResultPartsStorage
 * ----------------------------------------------
 */

/* Main trigger */
static void
prepare_rri_for_insert(ResultRelInfoHolder *rri_holder,
					   const ResultPartsStorage *rps_storage)
{
	prepare_rri_returning_for_insert(rri_holder, rps_storage);
	prepare_rri_fdw_for_insert(rri_holder, rps_storage);
}

/* Prepare 'RETURNING *' tlist & projection */
static void
prepare_rri_returning_for_insert(ResultRelInfoHolder *rri_holder,
								 const ResultPartsStorage *rps_storage)
{
	PartitionFilterState   *pfstate;
	List				   *returning_list;
	ResultRelInfo		   *child_rri,
						   *parent_rri;
	Index					parent_rt_idx;
	TupleTableSlot		   *result_slot;
	EState				   *estate;

	estate = rps_storage->estate;

	/* We don't need to do anything ff there's no map */
	if (!rri_holder->tuple_map)
		return;

	pfstate = (PartitionFilterState *) rps_storage->callback_arg;
	returning_list = pfstate->returning_list;

	/* Exit if there's no RETURNING list */
	if (!returning_list)
		return;

	child_rri = rri_holder->result_rel_info;
	parent_rri = rps_storage->saved_rel_info;
	parent_rt_idx = parent_rri->ri_RangeTableIndex;

	/* Create ExprContext for tuple projections */
	if (!pfstate->tup_convert_econtext)
		pfstate->tup_convert_econtext = CreateExprContext(estate);

	/* Replace parent's varattnos with child's */
	returning_list = (List *)
			fix_returning_list_mutator((Node *) returning_list,
									   list_make2(makeInteger(parent_rt_idx),
												  rri_holder));

	/* Specify tuple slot where will be place projection result in */
#if PG_VERSION_NUM >= 100000
	result_slot = parent_rri->ri_projectReturning->pi_state.resultslot;
#elif PG_VERSION_NUM >= 90500
	result_slot = parent_rri->ri_projectReturning->pi_slot;
#endif

	/* Build new projection info */
	child_rri->ri_projectReturning =
		ExecBuildProjectionInfoCompat(returning_list, pfstate->tup_convert_econtext,
									  result_slot, NULL /* HACK: no PlanState */,
									  RelationGetDescr(child_rri->ri_RelationDesc));
}

/* Prepare FDW access structs */
static void
prepare_rri_fdw_for_insert(ResultRelInfoHolder *rri_holder,
						   const ResultPartsStorage *rps_storage)
{
	ResultRelInfo  *rri = rri_holder->result_rel_info;
	FdwRoutine	   *fdw_routine = rri->ri_FdwRoutine;
	Oid				partid;
	EState		   *estate;

	estate = rps_storage->estate;

	/* Nothing to do if not FDW */
	if (fdw_routine == NULL)
		return;

	partid = RelationGetRelid(rri->ri_RelationDesc);

	/* Perform some checks according to 'pg_pathman_insert_into_fdw' */
	switch (pg_pathman_insert_into_fdw)
	{
		case PF_FDW_INSERT_DISABLED:
			elog(ERROR, "INSERTs into FDW partitions are disabled");
			break;

		case PF_FDW_INSERT_POSTGRES:
		case PF_FDW_INSERT_ANY_FDW:
			{
				ForeignDataWrapper *fdw;
				ForeignServer	   *fserver;

				/* Check if it's PostgreSQL FDW */
				fserver = GetForeignServer(GetForeignTable(partid)->serverid);
				fdw = GetForeignDataWrapper(fserver->fdwid);

				/* Show message if not postgres_fdw */
				if (strcmp("gogudb_fdw", fdw->fdwname) != 0)
					switch (pg_pathman_insert_into_fdw)
					{
						case PF_FDW_INSERT_POSTGRES:
							elog(ERROR,
								 "FDWs other than gogudb_fdw are restricted");

						case PF_FDW_INSERT_ANY_FDW:
							elog(WARNING,
								 "unrestricted FDW mode may lead to crashes");
					}
			}
			break;

		default:
			elog(ERROR, "Mode is not implemented yet");
			break;
	}

	if (fdw_routine->PlanForeignModify)
	{
		RangeTblEntry	   *rte;
		ModifyTableState	mtstate;
		List			   *fdw_private;
		Query				query;
		PlannedStmt		   *plan;
		TupleDesc			tupdesc;
		int					i,
							target_attr;
		PlanState		    plan_state;
		PlanState		   *plan_state_ptr;

		/* Fetch RangeTblEntry for partition */
		rte = rt_fetch(rri->ri_RangeTableIndex, estate->es_range_table);

		/* Fetch tuple descriptor */
		tupdesc = RelationGetDescr(rri->ri_RelationDesc);

		/* Create fake Query node */
		memset((void *) &query, 0, sizeof(Query));
		NodeSetTag(&query, T_Query);

		query.commandType		= CMD_INSERT;
		query.querySource		= QSRC_ORIGINAL;
		query.resultRelation	= 1;
		query.rtable			= list_make1(copyObject(rte));
		query.jointree			= makeNode(FromExpr);

		query.targetList		= NIL;
		query.returningList		= NIL;

		/* Generate 'query.targetList' using 'tupdesc' */
		target_attr = 1;
		for (i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute	attr;
			TargetEntry		   *te;
			Param			   *param;

#if PG_VERSION_NUM >= 110000
			attr = &(tupdesc->attrs[i]);
#else
			attr = tupdesc->attrs[i];
#endif
			if (attr->attisdropped)
				continue;

			param = makeNode(Param);
			param->paramkind	= PARAM_EXTERN;
			param->paramid		= target_attr;
			param->paramtype	= attr->atttypid;
			param->paramtypmod	= attr->atttypmod;
			param->paramcollid	= attr->attcollation;
			param->location		= -1;

			te = makeTargetEntry((Expr *) param, target_attr,
								 pstrdup(NameStr(attr->attname)),
								 false);

			query.targetList = lappend(query.targetList, te);

			target_attr++;
		}

		/* Create fake ModifyTableState */
		memset((void *) &mtstate, 0, sizeof(ModifyTableState));
		NodeSetTag(&mtstate, T_ModifyTableState);
		mtstate.ps.state = estate;
		mtstate.operation = CMD_INSERT;
		mtstate.resultRelInfo = rri;
#if PG_VERSION_NUM < 110000
		mtstate.mt_onconflict = ONCONFLICT_NONE;
#endif
		/* Plan fake query in for FDW access to be planned as well */
		elog(DEBUG1, "FDW(%u): plan fake query for fdw_private", partid);
		plan = standard_planner(&query, 0, NULL);

		/*
 		 ** Add fake PlanState to mt_plans. Though no one will ever look at the
 		 ** contents, postgresBeginForeignModify segfaults since 11 if it is
 		 ** absent. mt_plans is used when operation is CMD_UPDATE or CMD_DELETE.
 		 **/

		memset(&plan_state, 0, sizeof(PlanState));
		plan_state_ptr = &plan_state;
		mtstate.mt_plans = &plan_state_ptr;
		/* Extract fdw_private from useless plan */
		elog(DEBUG1, "FDW(%u): extract fdw_private", partid);
		fdw_private = (List *)
				linitial(((ModifyTable *) plan->planTree)->fdwPrivLists);

		/* call BeginForeignModify on 'rri' */
		elog(DEBUG1, "FDW(%u): call BeginForeignModify on a fake INSERT node", partid);
		fdw_routine->BeginForeignModify(&mtstate, rri, fdw_private, 0, 0);

		/* Report success */
		elog(DEBUG1, "FDW(%u): success", partid);
	}
}

/* Make parent's Vars of returninig list point to child's tuple */
static Node *
fix_returning_list_mutator(Node *node, void *state)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Var))
	{
		/* Extract packed args */
		List				   *state_args = (List *) state;
		Index					parent_idx = intVal(linitial(state_args));
		ResultRelInfoHolder	   *rri_holder = (ResultRelInfoHolder *) lsecond(state_args);
		Var					   *var;

		/* Copy base fields of Var */
		var = (Var *) palloc(sizeof(Var));
		*var = *(Var *) node;

		/* Make Var point to child's attribute */
		if (var->varno == parent_idx &&
			var->varattno >= 0) /* don't change sysattrs! */
		{
			int		i;
			bool	found_mapping = false;

			/* WHOLEROW reference, change row type */
			if (var->varattno == 0)
			{
				Relation child_rel = rri_holder->result_rel_info->ri_RelationDesc;

				/* Assign var->vartype a TupleDesc's type */
				var->vartype = RelationGetDescr(child_rel)->tdtypeid;

				return (Node *) var;
			}

			/* Map: child_att => parent_att, so we have to run through it */
			for (i = 0; i < rri_holder->tuple_map->outdesc->natts; i++)
			{
				/* Good, 'varattno' of parent is child's 'i+1' */
				if (var->varattno == rri_holder->tuple_map->attrMap[i])
				{
					var->varattno = i + 1; /* attnos begin with 1 */
					found_mapping = true;
					break;
				}
			}

			/* Swear if we couldn't find mapping for this attribute */
			if (!found_mapping)
				elog(ERROR, "could not bind attribute %d for returning statement",
							var->varattno);
		}

		return (Node *) var;
	}

	return expression_tree_mutator(node, fix_returning_list_mutator, state);
}


/*
 * -------------------------------------
 *  ExecutorState-related modifications
 * -------------------------------------
 */

/* Append RangeTblEntry 'rte' to estate->es_range_table */
static Index
append_rte_to_estate(EState *estate, RangeTblEntry *rte)
{
	estate_mod_data	   *emd_struct = fetch_estate_mod_data(estate);

	/* Copy estate->es_range_table if it's first time expansion */
	if (emd_struct->estate_not_modified)
		estate->es_range_table = list_copy(estate->es_range_table);

	estate->es_range_table = lappend(estate->es_range_table, rte);

	/* Update estate_mod_data */
	emd_struct->estate_not_modified = false;

	return list_length(estate->es_range_table);
}

/* Append ResultRelInfo 'rri' to estate->es_result_relations */
static int
append_rri_to_estate(EState *estate, ResultRelInfo *rri)
{
	estate_mod_data	   *emd_struct = fetch_estate_mod_data(estate);
	int					result_rels_allocated = emd_struct->estate_alloc_result_rels;

	/* Reallocate estate->es_result_relations if needed */
	if (result_rels_allocated <= estate->es_num_result_relations)
	{
		ResultRelInfo *rri_array = estate->es_result_relations;

		result_rels_allocated = result_rels_allocated * ALLOC_EXP + 1;
		estate->es_result_relations = palloc(result_rels_allocated *
												sizeof(ResultRelInfo));
		memcpy(estate->es_result_relations,
			   rri_array,
			   estate->es_num_result_relations * sizeof(ResultRelInfo));
	}

	/*
	 * Append ResultRelInfo to 'es_result_relations' array.
	 * NOTE: this is probably safe since ResultRelInfo
	 * contains nothing but pointers to various structs.
	 */
	estate->es_result_relations[estate->es_num_result_relations] = *rri;

	/* Update estate_mod_data */
	emd_struct->estate_alloc_result_rels = result_rels_allocated;
	emd_struct->estate_not_modified = false;

	return estate->es_num_result_relations++;
}


/*
 * --------------------------------------
 *  Store data in 'estate->es_query_cxt'
 * --------------------------------------
 */

/* Used by fetch_estate_mod_data() to find estate_mod_data */
static void
pf_memcxt_callback(void *arg) { elog(DEBUG1, "EState is destroyed"); }

/* Fetch (or create) a estate_mod_data structure we've hidden inside es_query_cxt */
static estate_mod_data *
fetch_estate_mod_data(EState *estate)
{
	MemoryContext			estate_mcxt = estate->es_query_cxt;
	estate_mod_data		   *emd_struct;
	MemoryContextCallback  *cb = estate_mcxt->reset_cbs;

	/* Go through callback list */
	while (cb != NULL)
	{
		/* This is the dummy callback we're looking for! */
		if (cb->func == pf_memcxt_callback)
			return (estate_mod_data *) cb->arg;

		cb = estate_mcxt->reset_cbs->next;
	}

	/* Have to create a new one */
	emd_struct = MemoryContextAlloc(estate_mcxt, sizeof(estate_mod_data));
	emd_struct->estate_not_modified = true;
	emd_struct->estate_alloc_result_rels = estate->es_num_result_relations;

	cb = MemoryContextAlloc(estate_mcxt, sizeof(MemoryContextCallback));
	cb->func = pf_memcxt_callback;
	cb->arg = emd_struct;

	MemoryContextRegisterResetCallback(estate_mcxt, cb);

	return emd_struct;
}
