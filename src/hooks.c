/* ------------------------------------------------------------------------
 *
 * hooks.c
 *		definitions of rel_pathlist and join_pathlist hooks
 *
 * Copyright (c) 2016, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */

#include "compat/pg_compat.h"
#include "compat/rowmarks_fix.h"

#include "hooks.h"
#include "pathman.h"
#include "init.h"
#include "partition_filter.h"
#include "pathman_workers.h"
#include "planner_tree_modification.h"
#include "runtimeappend.h"
#include "runtime_merge_append.h"
#include "utility_stmt_hooking.h"
#include "utils.h"
#include "xact_handling.h"
#include "connection_pool.h"
#include "libpq-fe.h"

#include "access/transam.h"
#include "access/stratnum.h"
#include "access/htup_details.h"

#include "catalog/pg_inherits.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"
#include "catalog/namespace.h"
#if PG_VERSION_NUM < 110000
#include "catalog/pg_inherits_fn.h"
#endif
#include "catalog/index.h"

#include "parser/parse_type.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "foreign/foreign.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "optimizer/prep.h"
#include "optimizer/clauses.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"
#include "rewrite/rewriteManip.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"
#include "executor/spi.h"
#include "funcapi.h"

#ifdef USE_ASSERT_CHECKING
#define USE_RELCACHE_LOGGING
#endif

/* Borrowed from joinpath.c */
#define PATH_PARAM_BY_REL(path, rel)  \
	((path)->param_info && bms_overlap(PATH_REQ_OUTER(path), (rel)->relids))

#define CMD_SPECIAL_BIT     ( (uint8) (((uint8)1) << 7) )
#define CMD_SPECIAL_MASK    ( (uint8) (~CMD_SPECIAL_BIT) )

#define is_cmd_special(cmd) ( ((cmd) & CMD_SPECIAL_BIT) > 0 )
#define set_cmd_special(cmd)    ( (uint8) (cmd) | CMD_SPECIAL_BIT )
#define unset_cmd_special(cmd)  ( (uint8) (cmd) & CMD_SPECIAL_MASK )

static RangeVar* get_parent_rangevar(RangeVar*child_rv); 

static List* get_remote_meta_4_child_foreign_table(RangeVar *rv);
 
static void handle_before_hook(Node* parsetree); 

static void handle_after_hook(Node* parsetree, const char* query);

static char *get_range_typename(List *tableElts, const char *column_name, bool *is_numeric);

static void handle_updatestmt(Query* parsetree);

static void handle_truncatestmt_before_hook(Node *parsetree);

static void handle_truncatestmt(Node *parsetree);

static void handle_createstmt(Node *parsetree, const char *queryString);

static void handle_create_index(Node *parsetree, const char* queryString);

static void handle_dropstmt(Node *parsetree);

static void handle_vacuumstmt(Node *parsetree, const char* queryString);

static void handle_clusterstmt(Node *parsetree);

static void handle_reindexstmt(Node* parsetree, const char* queryString);

static void handle_renamestmt(Node* parsetree);

static void handle_altertable_stmt(Node* parsetree, const char*queryString);

typedef struct remote_meta {
	UserMapping		*user;
	char	*local_schema;
	char	*local_table;
	char	*remote_schema;
	char	*remote_table;
} Remote_meta;


static inline bool
allow_star_schema_join(PlannerInfo *root,
					   Path *outer_path,
					   Path *inner_path)
{
	Relids		innerparams = PATH_REQ_OUTER(inner_path);
	Relids		outerrelids = outer_path->parent->relids;

	/*
	 * It's a star-schema case if the outer rel provides some but not all of
	 * the inner rel's parameterization.
	 */
	return (bms_overlap(innerparams, outerrelids) &&
			bms_nonempty_difference(innerparams, outerrelids));
}


set_join_pathlist_hook_type		set_join_pathlist_next = NULL;
set_rel_pathlist_hook_type		set_rel_pathlist_hook_next = NULL;
planner_hook_type				planner_hook_next = NULL;
post_parse_analyze_hook_type	post_parse_analyze_hook_next = NULL;
shmem_startup_hook_type			shmem_startup_hook_next = NULL;
ProcessUtility_hook_type		process_utility_hook_next = NULL;


/* Take care of joins */
void
pathman_join_pathlist_hook(PlannerInfo *root,
						   RelOptInfo *joinrel,
						   RelOptInfo *outerrel,
						   RelOptInfo *innerrel,
						   JoinType jointype,
						   JoinPathExtraData *extra)
{
	JoinCostWorkspace		workspace;
	JoinType				saved_jointype = jointype;
	RangeTblEntry		   *inner_rte = root->simple_rte_array[innerrel->relid];
	const PartRelationInfo *inner_prel;
	List				   *joinclauses,
						   *otherclauses;
	WalkerContext			context;
	double					paramsel;
	Node				   *part_expr;
	ListCell			   *lc;

	/* Call hooks set by other extensions */
	if (set_join_pathlist_next)
		set_join_pathlist_next(root, joinrel, outerrel,
							   innerrel, jointype, extra);

	/* Check that both pg_pathman & RuntimeAppend nodes are enabled */
	if (!IsPathmanReady() || !pg_pathman_enable_runtimeappend)
		return;

	/* We should only consider base relations */
	if (innerrel->reloptkind != RELOPT_BASEREL)
		return;

	/* We shouldn't process tables with active children */
	if (inner_rte->inh)
		return;

	/* We can't handle full or right outer joins */
	if (jointype == JOIN_FULL || jointype == JOIN_RIGHT)
		return;

	/* Check that innerrel is a BASEREL with PartRelationInfo */
	if (innerrel->reloptkind != RELOPT_BASEREL ||
		!(inner_prel = get_pathman_relation_info(inner_rte->relid)))
		return;

	/*
	 * Check if query is:
	 *		1) UPDATE part_table SET = .. FROM part_table.
	 *		2) DELETE FROM part_table USING part_table.
	 *
	 * Either outerrel or innerrel may be a result relation.
	 */
	if ((root->parse->resultRelation == outerrel->relid ||
		 root->parse->resultRelation == innerrel->relid) &&
			(root->parse->commandType == CMD_UPDATE ||
			 root->parse->commandType == CMD_DELETE))
	{
		int		rti = -1,
				count = 0;

		/* Inner relation must be partitioned */
		Assert(inner_prel);

		/* Check each base rel of outer relation */
		while ((rti = bms_next_member(outerrel->relids, rti)) >= 0)
		{
			Oid outer_baserel = root->simple_rte_array[rti]->relid;

			/* Is it partitioned? */
			if (get_pathman_relation_info(outer_baserel))
				count++;
		}

		if (count > 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("DELETE and UPDATE queries with a join "
							"of partitioned tables are not supported")));
	}

	/* Skip if inner table is not allowed to act as parent (e.g. FROM ONLY) */
	if (PARENTHOOD_DISALLOWED == get_rel_parenthood_status(inner_rte))
		return;

	/*
	 * These codes are used internally in the planner, but are not supported
	 * by the executor (nor, indeed, by most of the planner).
	 */
	if (jointype == JOIN_UNIQUE_OUTER || jointype == JOIN_UNIQUE_INNER)
		jointype = JOIN_INNER; /* replace with a proper value */

	/* Extract join clauses which will separate partitions */
	if (IS_OUTER_JOIN(extra->sjinfo->jointype))
	{
#if PG_VERSION_NUM >= 100004 || (PG_VERSION_NUM >= 90609 && PG_VERSION_NUM < 100000)
		extract_actual_join_clauses(extra->restrictlist, joinrel->relids,
									&joinclauses, &otherclauses);

#else
		extract_actual_join_clauses(extra->restrictlist,
									&joinclauses, &otherclauses);
#endif
	}
	else
	{
		/* We can treat all clauses alike for an inner join */
		joinclauses = extract_actual_clauses(extra->restrictlist, false);
		otherclauses = NIL;
	}

	/* Make copy of partitioning expression and fix Var's  varno attributes */
	part_expr = PrelExpressionForRelid(inner_prel, innerrel->relid);

	paramsel = 1.0;
	foreach (lc, joinclauses)
	{
		WrapperNode *wrap;

		InitWalkerContext(&context, part_expr, inner_prel, NULL);
		wrap = walk_expr_tree((Expr *) lfirst(lc), &context);
		paramsel *= wrap->paramsel;
	}

	foreach (lc, innerrel->pathlist)
	{
		AppendPath	   *cur_inner_path = (AppendPath *) lfirst(lc);
		Path		   *outer,
					   *inner;
		NestPath	   *nest_path;		/* NestLoop we're creating */
		ParamPathInfo  *ppi;			/* parameterization info */
		Relids			required_nestloop,
						required_inner;
		List		   *filtered_joinclauses = NIL,
					   *saved_ppi_list,
					   *pathkeys;
		ListCell	   *rinfo_lc;

		if (!IsA(cur_inner_path, AppendPath))
			continue;

		/* Select cheapest path for outerrel */
		outer = outerrel->cheapest_total_path;

		/* We cannot use an outer path that is parameterized by the inner rel */
		if (PATH_PARAM_BY_REL(outer, innerrel))
			continue;

		/* Wrap 'outer' in unique path if needed */
		if (saved_jointype == JOIN_UNIQUE_OUTER)
		{
			outer = (Path *) create_unique_path(root, outerrel,
												outer, extra->sjinfo);
			Assert(outer);
		}

		 /* No way to do this in a parameterized inner path */
		if (saved_jointype == JOIN_UNIQUE_INNER)
			return;


		/* Make inner path depend on outerrel's columns */
		required_inner = bms_union(PATH_REQ_OUTER((Path *) cur_inner_path),
								   outerrel->relids);

		/* Preserve existing ppis built by get_appendrel_parampathinfo() */
		saved_ppi_list = innerrel->ppilist;

		/* Get the ParamPathInfo for a parameterized path */
		innerrel->ppilist = NIL;
		ppi = get_baserel_parampathinfo(root, innerrel, required_inner);
		innerrel->ppilist = saved_ppi_list;

		/* Skip ppi->ppi_clauses don't reference partition attribute */
		if (!(ppi && get_partitioning_clauses(ppi->ppi_clauses,
											  inner_prel,
											  innerrel->relid)))
			continue;

		inner = create_runtimeappend_path(root, cur_inner_path, ppi, paramsel);
		if (!inner)
			return; /* could not build it, retreat! */

#if PG_VERSION_NUM >= 110000
		required_nestloop = calc_nestloop_required_outer(outer->parent->top_parent_relids ?
								outer->parent->top_parent_relids :
								outer->parent->relids, PATH_REQ_OUTER(outer),
								inner->parent->top_parent_relids ?
								inner->parent->top_parent_relids :
								inner->parent->relids, PATH_REQ_OUTER(inner));
#else
		required_nestloop = calc_nestloop_required_outer(outer, inner);
#endif
		/*
		 * Check to see if proposed path is still parameterized, and reject if the
		 * parameterization wouldn't be sensible --- unless allow_star_schema_join
		 * says to allow it anyway.  Also, we must reject if have_dangerous_phv
		 * doesn't like the look of it, which could only happen if the nestloop is
		 * still parameterized.
		 */
		if (required_nestloop &&
			((!bms_overlap(required_nestloop, extra->param_source_rels) &&
			  !allow_star_schema_join(root, outer, inner)) ||
			 have_dangerous_phv(root, outer->parent->relids, required_inner)))
			return;

		initial_cost_nestloop_compat(root, &workspace, jointype, outer, inner, extra);

		pathkeys = build_join_pathkeys(root, joinrel, jointype, outer->pathkeys);

		/* Discard all clauses that are to be evaluated by 'inner' */
		foreach (rinfo_lc, extra->restrictlist)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(rinfo_lc);

			Assert(IsA(rinfo, RestrictInfo));
			if (!join_clause_is_movable_to(rinfo, inner->parent))
				filtered_joinclauses = lappend(filtered_joinclauses, rinfo);
		}

		nest_path =
			create_nestloop_path_compat(root, joinrel, jointype,
										&workspace, extra, outer, inner,
										filtered_joinclauses, pathkeys,
										required_nestloop);

		/*
		 * NOTE: Override 'rows' value produced by standard estimator.
		 * Currently we use get_parameterized_joinrel_size() since
		 * it works just fine, but this might change some day.
		 */
		nest_path->path.rows =
				get_parameterized_joinrel_size_compat(root, joinrel,
													  outer, inner,
													  extra->sjinfo,
													  filtered_joinclauses);

		/* Finally we can add the new NestLoop path */
		add_path(joinrel, (Path *) nest_path);
	}
}

/* Cope with simple relations */
void
pathman_rel_pathlist_hook(PlannerInfo *root,
						  RelOptInfo *rel,
						  Index rti,
						  RangeTblEntry *rte)
{
	const PartRelationInfo *prel;
	int						irange_len;

	/* Invoke original hook if needed */
	if (set_rel_pathlist_hook_next != NULL)
		set_rel_pathlist_hook_next(root, rel, rti, rte);

	/* Make sure that pg_pathman is ready */
	if (!IsPathmanReady())
		return;

	/* We shouldn't process tables with active children */
	if (rte->inh)
		return;

	/*
	 * Skip if it's a result relation (UPDATE | DELETE | INSERT),
	 * or not a (partitioned) physical relation at all.
	 */
	if (rte->rtekind != RTE_RELATION ||
		rte->relkind != RELKIND_RELATION ||
		root->parse->resultRelation == rti)
		return;

#ifdef LEGACY_ROWMARKS_95
	/* It's better to exit, since RowMarks might be broken */
	if (root->parse->commandType != CMD_SELECT &&
		root->parse->commandType != CMD_INSERT)
		return;
#endif

	/* Skip if this table is not allowed to act as parent (e.g. FROM ONLY) */
	if (PARENTHOOD_DISALLOWED == get_rel_parenthood_status(rte))
		return;

	/* Proceed iff relation 'rel' is partitioned */
	if ((prel = get_pathman_relation_info(rte->relid)) != NULL)
	{
		Relation		parent_rel;				/* parent's relation (heap) */
		PlanRowMark	   *parent_rowmark;			/* parent's rowmark */
		Oid			   *children;				/* selected children oids */
		List		   *ranges,					/* a list of IndexRanges */
					   *wrappers;				/* a list of WrapperNodes */
		PathKey		   *pathkeyAsc = NULL,
					   *pathkeyDesc = NULL;
		double			paramsel = 1.0;			/* default part selectivity */
		WalkerContext	context;
		Node		   *part_expr;
		List		   *part_clauses;
		ListCell	   *lc;
		int				i;

		/*
		 * Check that this child is not the parent table itself.
		 * This is exactly how standard inheritance works.
		 *
		 * Helps with queries like this one:
		 *
		 *		UPDATE test.tmp t SET value = 2
		 *		WHERE t.id IN (SELECT id
		 *					   FROM test.tmp2 t2
		 *					   WHERE id = t.id);
		 *
		 * Since we disable optimizations on 9.5, we
		 * have to skip parent table that has already
		 * been expanded by standard inheritance.
		 */
		if (rel->reloptkind == RELOPT_OTHER_MEMBER_REL)
		{
			foreach (lc, root->append_rel_list)
			{
				AppendRelInfo  *appinfo = (AppendRelInfo *) lfirst(lc);
				RangeTblEntry  *cur_parent_rte,
							   *cur_child_rte;

				/*  This 'appinfo' is not for this child */
				if (appinfo->child_relid != rti)
					continue;

				cur_parent_rte = root->simple_rte_array[appinfo->parent_relid];
				cur_child_rte  = rte; /* we already have it, saves time */

				/* This child == its own parent table! */
				if (cur_parent_rte->relid == cur_child_rte->relid)
					return;
			}
		}

		/* Make copy of partitioning expression and fix Var's  varno attributes */
		part_expr = PrelExpressionForRelid(prel, rti);

		/* Get partitioning-related clauses (do this before append_child_relation()) */
		part_clauses = get_partitioning_clauses(rel->baserestrictinfo, prel, rti);

		if (prel->parttype == PT_RANGE)
		{
			/*
			 * Get pathkeys for ascending and descending sort by partitioned column.
			 */
			List		   *pathkeys;
			TypeCacheEntry *tce;

			/* Determine operator type */
			tce = lookup_type_cache(prel->ev_type, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

			/* Make pathkeys */
			pathkeys = build_expression_pathkey(root, (Expr *) part_expr, NULL,
												tce->lt_opr, NULL, false);
			if (pathkeys)
				pathkeyAsc = (PathKey *) linitial(pathkeys);
			pathkeys = build_expression_pathkey(root, (Expr *) part_expr, NULL,
												tce->gt_opr, NULL, false);
			if (pathkeys)
				pathkeyDesc = (PathKey *) linitial(pathkeys);
		}

		children = PrelGetChildrenArray(prel);
		ranges = list_make1_irange_full(prel, IR_COMPLETE);

		/* Make wrappers over restrictions and collect final rangeset */
		InitWalkerContext(&context, part_expr, prel, NULL);
		wrappers = NIL;
		foreach(lc, rel->baserestrictinfo)
		{
			WrapperNode	   *wrap;
			RestrictInfo   *rinfo = (RestrictInfo *) lfirst(lc);

			wrap = walk_expr_tree(rinfo->clause, &context);

			paramsel *= wrap->paramsel;
			wrappers = lappend(wrappers, wrap);
			ranges = irange_list_intersection(ranges, wrap->rangeset);
		}

		/* Get number of selected partitions */
		irange_len = irange_list_length(ranges);
		if (prel->enable_parent)
			irange_len++; /* also add parent */

		/* Expand simple_rte_array and simple_rel_array */
		if (irange_len > 0)
		{
			int current_len	= root->simple_rel_array_size,
				new_len		= current_len + irange_len;

			/* Expand simple_rel_array */
			root->simple_rel_array = (RelOptInfo **)
					repalloc(root->simple_rel_array,
							 new_len * sizeof(RelOptInfo *));

			memset((void *) &root->simple_rel_array[current_len], 0,
				   irange_len * sizeof(RelOptInfo *));

			/* Expand simple_rte_array */
			root->simple_rte_array = (RangeTblEntry **)
					repalloc(root->simple_rte_array,
							 new_len * sizeof(RangeTblEntry *));

			memset((void *) &root->simple_rte_array[current_len], 0,
				   irange_len * sizeof(RangeTblEntry *));

			/* Don't forget to update array size! */
			root->simple_rel_array_size = new_len;
		}

#if PG_VERSION_NUM >= 110000
		{
			int size = root->simple_rel_array_size;
			if (prel->enable_parent)
				size++;

			if (root->append_rel_array == NULL) {
				root->append_rel_array = (AppendRelInfo **)
							palloc0(size * sizeof(AppendRelInfo *));
			} else {
				int old_size = size - irange_len;
				if (prel->enable_parent)
					old_size--;

				root->append_rel_array = (AppendRelInfo **)
							repalloc(root->append_rel_array, 
								size * sizeof(AppendRelInfo *));
				memset((void *) &root->append_rel_array[old_size], 0,
					(size - old_size) * sizeof(AppendRelInfo *));

			}
		}		
#endif

		/* Parent has already been locked by rewriter */
		parent_rel = heap_open(rte->relid, NoLock);

		parent_rowmark = get_plan_rowmark(root->rowMarks, rti);

		/*
		 * WARNING: 'prel' might become invalid after append_child_relation().
		 */

		/* Add parent if asked to */
		if (prel->enable_parent)
			append_child_relation(root, parent_rel, parent_rowmark,
								  rti, 0, rte->relid, NULL);

		/* Iterate all indexes in rangeset and append child relations */
		foreach(lc, ranges)
		{
			IndexRange irange = lfirst_irange(lc);

			for (i = irange_lower(irange); i <= irange_upper(irange); i++)
				append_child_relation(root, parent_rel, parent_rowmark,
									  rti, i, children[i], wrappers);
		}

		/* Now close parent relation */
		heap_close(parent_rel, NoLock);

		/* Clear path list and make it point to NIL */
		list_free_deep(rel->pathlist);
		rel->pathlist = NIL;

#if PG_VERSION_NUM >= 90600
		/* Clear old partial path list */
		list_free(rel->partial_pathlist);
		rel->partial_pathlist = NIL;
#endif

		/* Generate new paths using the rels we've just added */
		set_append_rel_pathlist(root, rel, rti, pathkeyAsc, pathkeyDesc);
		set_append_rel_size_compat(root, rel, rti);

#if PG_VERSION_NUM >= 110000
		/* consider gathering partial paths for the parent appendrel */
		generate_gather_paths(root, rel, false);
#elif PG_VERSION_NUM >= 90600
		/* consider gathering partial paths for the parent appendrel */
		generate_gather_paths(root, rel);
#endif

		/* No need to go further (both nodes are disabled), return */
		if (!(pg_pathman_enable_runtimeappend ||
			  pg_pathman_enable_runtime_merge_append))
			return;

		/* Skip if there's no PARAMs in partitioning-related clauses */
		if (!clause_contains_params((Node *) part_clauses))
			return;

		/* Generate Runtime[Merge]Append paths if needed */
		foreach (lc, rel->pathlist)
		{
			AppendPath	   *cur_path = (AppendPath *) lfirst(lc);
			Relids			inner_required = PATH_REQ_OUTER((Path *) cur_path);
			Path		   *inner_path = NULL;
			ParamPathInfo  *ppi;

			/* Skip if rel contains some join-related stuff or path type mismatched */
			if (!(IsA(cur_path, AppendPath) || IsA(cur_path, MergeAppendPath)) ||
				rel->has_eclass_joins || rel->joininfo)
			{
				continue;
			}

			/* Get existing parameterization */
			ppi = get_appendrel_parampathinfo(rel, inner_required);

			if (IsA(cur_path, AppendPath) && pg_pathman_enable_runtimeappend)
				inner_path = create_runtimeappend_path(root, cur_path,
													   ppi, paramsel);
			else if (IsA(cur_path, MergeAppendPath) &&
					 pg_pathman_enable_runtime_merge_append)
			{
				/* Check struct layout compatibility */
				if (offsetof(AppendPath, subpaths) !=
						offsetof(MergeAppendPath, subpaths))
					elog(FATAL, "Struct layouts of AppendPath and "
								"MergeAppendPath differ");

				inner_path = create_runtimemergeappend_path(root, cur_path,
															ppi, paramsel);
			}

			if (inner_path)
				add_path(rel, inner_path);
		}
	}
}

/*
 * Intercept 'pg_pathman.enable' GUC assignments.
 */
void
pathman_enable_assign_hook(bool newval, void *extra)
{
	/*elog(DEBUG2, "pg_pathman_enable_assign_hook() [newval = %s] triggered",
		  newval ? "true" : "false");*/

	/* Return quickly if nothing has changed */
	if (newval == (pathman_init_state.pg_pathman_enable &&
				   pathman_init_state.auto_partition &&
				   pathman_init_state.override_copy &&
				   pg_pathman_enable_runtimeappend &&
				   pg_pathman_enable_runtime_merge_append &&
				   pg_pathman_enable_partition_filter &&
				   pg_pathman_enable_bounds_cache))
		return;

	pathman_init_state.auto_partition		= newval;
	pathman_init_state.override_copy		= newval;
	pg_pathman_enable_runtimeappend			= newval;
	pg_pathman_enable_runtime_merge_append	= newval;
	pg_pathman_enable_partition_filter		= newval;
	pg_pathman_enable_bounds_cache			= newval;

	elog(NOTICE,
		 "RuntimeAppend, RuntimeMergeAppend and PartitionFilter nodes "
		 "and some other options have been %s",
		 newval ? "enabled" : "disabled");
}

/*
 * Planner hook. It disables inheritance for tables that have been partitioned
 * by pathman to prevent standard PostgreSQL partitioning mechanism from
 * handling that tables.
 */
PlannedStmt *
pathman_planner_hook(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
#define ExecuteForPlanTree(planned_stmt, proc) \
	do { \
		ListCell *lc; \
		proc((planned_stmt)->rtable, (planned_stmt)->planTree); \
		foreach (lc, (planned_stmt)->subplans) \
			proc((planned_stmt)->rtable, (Plan *) lfirst(lc)); \
	} while (0)

	PlannedStmt	   *result;
	uint32			query_id = parse->queryId;

	/* Save the result in case it changes */
	bool			pathman_ready = IsPathmanReady();

	PG_TRY();
	{
		if (pathman_ready)
		{
			/* Increase planner() calls count */
			incr_planner_calls_count();
			if (parse->commandType == CMD_UPDATE)
				handle_updatestmt(parse);
				
			/* Modify query tree if needed */
			pathman_transform_query(parse, boundParams);
		}

		/* Invoke original hook if needed */
		if (planner_hook_next)
			result = planner_hook_next(parse, cursorOptions, boundParams);
		else
			result = standard_planner(parse, cursorOptions, boundParams);

		if (pathman_ready)
		{
			/* Add PartitionFilter node for INSERT queries */
			ExecuteForPlanTree(result, add_partition_filters);

			/* Decrement planner() calls count */
			decr_planner_calls_count();

			/* HACK: restore queryId set by pg_stat_statements */
			result->queryId = query_id;
		}
	}
	/* We must decrease parenthood statuses refcount on ERROR */
	PG_CATCH();
	{
		if (pathman_ready)
		{
			/* Caught an ERROR, decrease count */
			decr_planner_calls_count();
		}

		/* Rethrow ERROR further */
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Finally return the Plan */
	return result;
}

/*
 * Post parse analysis hook. It makes sure the config is loaded before executing
 * any statement, including utility commands
 */
void
pathman_post_parse_analysis_hook(ParseState *pstate, Query *query)
{
	/* Invoke original hook if needed */
	if (post_parse_analyze_hook_next)
		post_parse_analyze_hook_next(pstate, query);

	/* See cook_partitioning_expression() */
	if (!pathman_hooks_enabled)
		return;

	/* We shouldn't proceed on: ... */
	if (query->commandType == CMD_UTILITY)
	{
		/* ... BEGIN */
		if (xact_is_transaction_stmt(query->utilityStmt))
			return;

		/* ... SET pg_pathman.enable */
		if (xact_is_set_stmt(query->utilityStmt, PATHMAN_ENABLE))
		{
			/* Accept all events in case it's "enable = OFF" */
			if (IsPathmanReady())
				finish_delayed_invalidation();

			return;
		}

		/* ... SET [TRANSACTION] */
		if (xact_is_set_stmt(query->utilityStmt, NULL))
			return;

		/* ... ALTER EXTENSION pg_pathman */
		if (xact_is_alter_pathman_stmt(query->utilityStmt))
		{
			/* Leave no delayed events before ALTER EXTENSION */
			if (IsPathmanReady())
				finish_delayed_invalidation();

			/* Disable pg_pathman to perform a painless update */
			(void) set_config_option(PATHMAN_ENABLE, "off",
									 PGC_SUSET, PGC_S_SESSION,
									 GUC_ACTION_SAVE, true, 0, false);

			return;
		}
	}

	/* Finish all delayed invalidation jobs */
	if (IsPathmanReady())
		finish_delayed_invalidation();

	/* Load config if pg_pathman exists & it's still necessary */
	if (IsPathmanEnabled() &&
		!IsPathmanInitialized() &&
		/* Now evaluate the most expensive clause */
		get_pathman_schema() != InvalidOid)
	{
		load_config();
	}

	/* Process inlined SQL functions (we've already entered planning stage) */
	if (IsPathmanReady() && get_planner_calls_count() > 0)
	{
		/* Check that pg_pathman is the last extension loaded */
		if (post_parse_analyze_hook != pathman_post_parse_analysis_hook)
		{
			Oid		save_userid;
			int		save_sec_context;
			bool	need_priv_escalation = !superuser(); /* we might be a SU */
			char   *spl_value; /* value of "shared_preload_libraries" GUC */

			/* Do we have to escalate privileges? */
			if (need_priv_escalation)
			{
				/* Get current user's Oid and security context */
				GetUserIdAndSecContext(&save_userid, &save_sec_context);

				/* Become superuser in order to bypass sequence ACL checks */
				SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
									   save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
			}

			/* TODO: add a test for this case (non-privileged user etc) */

			/* Only SU can read this GUC */
#if PG_VERSION_NUM >= 90600
			spl_value = GetConfigOptionByName("shared_preload_libraries", NULL, false);
#else
			spl_value = GetConfigOptionByName("shared_preload_libraries", NULL);
#endif

			/* Restore user's privileges */
			if (need_priv_escalation)
				SetUserIdAndSecContext(save_userid, save_sec_context);

			ereport(ERROR,
					(errmsg("extension conflict has been detected"),
					 errdetail("shared_preload_libraries = \"%s\"", spl_value),
					 errhint("gogudb should be the last extension listed in "
							 "\"shared_preload_libraries\" GUC in order to "
							 "prevent possible conflicts with other extensions")));
		}

		/* Modify query tree if needed */
		pathman_transform_query(query, NULL);
	}
}

/*
 * Initialize dsm_config & shmem_config.
 */
void
pathman_shmem_startup_hook(void)
{
	/* Invoke original hook if needed */
	if (shmem_startup_hook_next != NULL)
		shmem_startup_hook_next();

	/* Allocate shared memory objects */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	init_concurrent_part_task_slots();
	LWLockRelease(AddinShmemInitLock);
}

/*
 * Invalidate PartRelationInfo cache entry if needed.
 */
void
pathman_relcache_hook(Datum arg, Oid relid)
{
	Oid parent_relid;

	/* See cook_partitioning_expression() */
	if (!pathman_hooks_enabled)
		return;

	if (!IsPathmanReady())
		return;

	/* Special case: flush whole relcache */
	if (relid == InvalidOid)
	{
		delay_invalidation_whole_cache();

#ifdef USE_RELCACHE_LOGGING
		elog(DEBUG2, "Invalidation message for all relations [%u]", MyProcPid);
#endif

		return;
	}

	/* We shouldn't even consider special OIDs */
	if (relid < FirstNormalObjectId)
		return;

	/* Invalidation event for PATHMAN_CONFIG table (probably DROP) */
	if (relid == get_pathman_config_relid(false))
		delay_pathman_shutdown();

	/* Invalidate PartBoundInfo cache if needed */
	forget_bounds_of_partition(relid);

	/* Invalidate PartParentInfo cache if needed */
	parent_relid = forget_parent_of_partition(relid, NULL);

	/* It *might have been a partition*, invalidate parent */
	if (OidIsValid(parent_relid))
	{
		delay_invalidation_parent_rel(parent_relid);

#ifdef USE_RELCACHE_LOGGING
		elog(DEBUG2, "Invalidation message for partition %u [%u]",
			 relid, MyProcPid);
#endif
	}
	/* We can't say, perform full invalidation procedure */
	else
	{
		delay_invalidation_vague_rel(relid);

#ifdef USE_RELCACHE_LOGGING
		elog(DEBUG2, "Invalidation message for vague rel %u [%u]",
			 relid, MyProcPid);
#endif
	}
}

/*
 * Utility function invoker hook.
 * NOTE: 'first_arg' is (PlannedStmt *) in PG 10, or (Node *) in PG <= 9.6.
 */
void
#if PG_VERSION_NUM >= 100000
pathman_process_utility_hook(PlannedStmt *first_arg,
							 const char *queryString,
							 ProcessUtilityContext context,
							 ParamListInfo params,
							 QueryEnvironment *queryEnv,
							 DestReceiver *dest, char *completionTag)
{
	Node   *parsetree		= first_arg->utilityStmt;
	int		stmt_location	= first_arg->stmt_location,
			stmt_len		= first_arg->stmt_len;
#else
pathman_process_utility_hook(Node *first_arg,
							 const char *queryString,
							 ProcessUtilityContext context,
							 ParamListInfo params,
							 DestReceiver *dest,
							 char *completionTag)
{
	Node   *parsetree		= first_arg;
	int		stmt_location	= -1,
			stmt_len		= 0;
#endif

	if (IsPathmanReady())
	{
		Oid			relation_oid;
		PartType	part_type;
		AttrNumber	attr_number;
		bool		is_parent;

		/* Override standard COPY statement if needed */
		if (is_pathman_related_copy(parsetree))
		{
			uint64	processed;

			/* Handle our COPY case (and show a special cmd name) */
			PathmanDoCopy((CopyStmt *) parsetree, queryString,
						  stmt_location, stmt_len, &processed);
			if (completionTag)
				snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
						 "COPY " UINT64_FORMAT, processed);

			return; /* don't call standard_ProcessUtility() or hooks */
		}

		/* Override standard RENAME statement if needed */
		else if (is_pathman_related_table_rename(parsetree,
												 &relation_oid,
												 &is_parent))
		{
			const RenameStmt *rename_stmt = (const RenameStmt *) parsetree;

			if (is_parent)
				PathmanRenameSequence(relation_oid, rename_stmt);
			else
				PathmanRenameConstraint(relation_oid, rename_stmt);
		}

		/* Override standard ALTER COLUMN TYPE statement if needed */
		else if (is_pathman_related_alter_column_type(parsetree,
													  &relation_oid,
													  &attr_number,
													  &part_type))
		{
			if (part_type == PT_HASH)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot change type of column \"%s\""
								" of table \"%s\" partitioned by HASH",
#if PG_VERSION_NUM >= 110000
								get_attname(relation_oid, attr_number, false),
#else
								get_attname(relation_oid, attr_number),
#endif
								get_rel_name(relation_oid))));

			/* Don't forget to invalidate parsed partitioning expression */
			pathman_config_invalidate_parsed_expression(relation_oid);
		}
	}

	if (IsPathmanReady())
	{

		handle_before_hook(parsetree);
	}

	/* Finally call process_utility_hook_next or standard_ProcessUtility */
	call_process_utility_compat((process_utility_hook_next ?
										process_utility_hook_next :
										standard_ProcessUtility),
								first_arg, queryString,
								context, params, queryEnv,
								dest, completionTag);

	if (IsPathmanReady())
	{
		handle_after_hook(parsetree, queryString);
	}
}

/*
 *	Try to find the direct parent rangevar, if no parent, return NULL;
 * */
static RangeVar  *get_parent_rangevar( RangeVar  *child_rv) 
{
	Relation        child_rel, inhrel, pg_class;
	Oid  child_id = InvalidOid, parent_sid = InvalidOid,parent_id = InvalidOid;
	RangeVar  *parent_rv = NULL;
	Form_pg_inherits inh;
	HeapTuple   inh_tup, parent_tup;
	ScanKeyData skey;
	SysScanDesc inhscan;

	child_rel = heap_openrv(child_rv, AccessShareLock);
	child_id = RelationGetRelid(child_rel);
	heap_close(child_rel, AccessShareLock);

   	inhrel = heap_open(InheritsRelationId, AccessShareLock);
   	ScanKeyInit(&skey, Anum_pg_inherits_inhrelid, BTEqualStrategyNumber,
                                F_OIDEQ, ObjectIdGetDatum(child_id));
   	inhscan = systable_beginscan(inhrel, InheritsRelidSeqnoIndexId, true,
                                                          NULL, 1, &skey);
	inh_tup = systable_getnext(inhscan);
	if (HeapTupleIsValid(inh_tup))
	{
		inh = (Form_pg_inherits) GETSTRUCT(inh_tup);
		parent_id = inh->inhparent;		
	}

	systable_endscan(inhscan);
	heap_close(inhrel, AccessShareLock);
	
	if (!OidIsValid(parent_id)) {
		return NULL;
	}

	pg_class = heap_open(RelationRelationId, AccessShareLock);

	/* Fetch heap tuple */
	parent_tup = SearchSysCache1(RELOID, ObjectIdGetDatum(parent_id));
	if (!HeapTupleIsValid(parent_tup)) {
		elog(ERROR, "cache lookup failed for relation %u", parent_id);
	} else {
		Relation        ns_relation;
		HeapTuple       ns_tup;
		Form_pg_class rd_rel = (Form_pg_class) GETSTRUCT(parent_tup);
		parent_sid = rd_rel->relnamespace;

		ns_relation = heap_open(NamespaceRelationId, AccessShareLock);

		ns_tup = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(parent_sid));
		if (!HeapTupleIsValid(ns_tup)) /* should not happen */
			elog(ERROR, "cache lookup failed for namespace %u", parent_sid);
		else {
			Form_pg_namespace nspForm = (Form_pg_namespace) GETSTRUCT(ns_tup);
			parent_rv = makeRangeVar(pstrdup(NameStr(nspForm->nspname)),
									pstrdup(NameStr(rd_rel->relname)), -1);
		}

		ReleaseSysCache(ns_tup);
		heap_close(ns_relation, AccessShareLock);
	}

	ReleaseSysCache(parent_tup);
	heap_close(pg_class, AccessShareLock);
	return parent_rv;
}

/*
 *	Do some work before standard_ProcessUtility
 * */
static void handle_before_hook(Node *parsetree) 
{

	switch (nodeTag(parsetree))
	{
		case T_AlterTableStmt:
			{
				AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
				RangeVar    *rv = stmt->relation;
				bool is_foreign_table = false;
				Relation rel = heap_openrv(rv, AccessShareLock);
				is_foreign_table = (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE);
				heap_close(rel, AccessShareLock);

				if (stmt->relkind == OBJECT_FOREIGN_TABLE || is_foreign_table) {
					ListCell   *lcmd;
					foreach(lcmd, stmt->cmds)
					{
						AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);
						RangeVar  *parent_rv = NULL;
						/* modify option of foreign table */
						if (cmd->subtype == AT_GenericOptions)
							return ;

						parent_rv = get_parent_rangevar(rv);			
						if (parent_rv && 
							read_table_partition_rule_params(parent_rv->schemaname,
															parent_rv->relname, NULL, NULL)) {
							elog(ERROR, "Cannot alter foreign table %s because it depended by %s.%s",
								rv->relname, parent_rv->schemaname, parent_rv->relname);
						}
					}
				}
			}
			break;

		case T_DropStmt:
			{
				handle_dropstmt(parsetree);
			}
			break;

		case T_RenameStmt:
			{
				handle_renamestmt(parsetree);		
			}
			break;

		case T_CreateStmt:
			{
				CreateStmt *stmt = (CreateStmt *)parsetree;
				RangeVar    *rv = stmt->relation;
				char		*schemaname = rv->schemaname;

				if (schemaname == NULL) {
					schemaname = get_namespace_name(RangeVarGetAndCheckCreationNamespace(rv, NoLock, NULL));
				}

				if (read_table_partition_rule_params(schemaname, rv->relname, NULL, NULL)
					&& rv->relpersistence==RELPERSISTENCE_TEMP) {
					elog(ERROR, "Cannot create temp table %s.%s because it is defined in partition rule table",
					rv->schemaname, rv->relname);
				}
			}
			break;

		case T_CreateTableAsStmt:
			{
				CreateTableAsStmt  *stmt = (CreateTableAsStmt *) parsetree;
				RangeVar    *rv = stmt->into->rel;
				char		*schemaname = rv->schemaname;

				if (schemaname == NULL) {
					schemaname = get_namespace_name(RangeVarGetAndCheckCreationNamespace(rv, NoLock, NULL));
				}

				if (read_table_partition_rule_params(schemaname, rv->relname, NULL, NULL)) {
					elog(ERROR, "Cannot create table %s.%s by CreateTableAsStmt",
						 schemaname, rv->relname);
				}
			}
			break;

		case T_AlterObjectSchemaStmt:
			{
				AlterObjectSchemaStmt *stmt = (AlterObjectSchemaStmt *) parsetree;

				if (stmt->objectType == OBJECT_TABLE){
					RangeVar    *rv = stmt->relation;
					char		*schemaname = rv->schemaname;

					if (schemaname == NULL) {
						Relation rel = heap_openrv(rv, AccessShareLock);
						schemaname = get_namespace_name(RelationGetNamespace(rel));
						heap_close(rel, AccessShareLock);
					}

					if (read_table_partition_rule_params(schemaname,
														 rv->relname, NULL, NULL)) {
						elog(ERROR, "Cannot alter schema of %s.%s, because it "
							"is defined in table partition rule ", schemaname, rv->relname);
					} else if (read_table_partition_rule_params(stmt->newschema,
															rv->relname, NULL, NULL)) {
						elog(ERROR, "Cannot alter table %s schema to %s, because it "
							"is defined in table partition rule", rv->relname, stmt->newschema);
					}
				}

			}
			break;

		case T_TruncateStmt:
			{
				handle_truncatestmt_before_hook(parsetree);
			}
			break;

		default:
			break;
	}
}

/*
 *	Try to get the children meta data for specified rangevar
 * */
static List* get_remote_meta_4_child_foreign_table(RangeVar *rv) 
{
	List       *children, *result = NULL;
	ListCell   *child;
	Relation	cur_rel = heap_openrv(rv, AccessShareLock);
	Oid			cur_id = RelationGetRelid(cur_rel);
	heap_close(cur_rel, AccessShareLock);
	children = find_inheritance_children(cur_id, AccessExclusiveLock);
	foreach(child, children)
	{
		ListCell   *option;
		Oid childrelid = lfirst_oid(child);
		ForeignTable *ftable = GetForeignTable(childrelid);
		Relation	child_rel = heap_open(ftable->relid, AccessShareLock);
		Remote_meta *meta_cell = (Remote_meta*)palloc0(sizeof(Remote_meta));

		meta_cell->local_schema = get_namespace_name(RelationGetNamespace(child_rel));
		meta_cell->local_table = RelationGetRelationName(child_rel);
		meta_cell->user = GetUserMapping(child_rel->rd_rel->relowner, ftable->serverid);
		heap_close(child_rel, AccessShareLock);
		foreach(option, ftable->options) 
		{
			DefElem    *def = (DefElem *) lfirst(option);
			if (strcmp(def->defname, "schema_name") == 0)
				meta_cell->remote_schema = defGetString(def);
			else if (strcmp(def->defname, "table_name") == 0)
				meta_cell->remote_table = defGetString(def);
		}
		
		result = lappend(result, meta_cell);
	}
	return result;
}

/*
 *	For range partition, need to get the data type of the parition column
 * */
static char *get_range_typename(List * tableElts, const char *column_name, bool *is_numeric)
{
	ListCell * cell;
	char	 * typename = NULL;
	StringInfoData namebuff;

	foreach(cell, tableElts)
	{
		ColumnDef  *colDef = lfirst(cell);
		if (strcmp(colDef->colname, column_name) == 0)
		{
			ListCell * name_cell;
			bool	 first = true;

			char typeDomain = '\0';
			Oid  typeOid = colDef->typeName->typeOid;
			if (InvalidOid ==typeOid)
				typeOid =  typenameTypeId(NULL, colDef->typeName);

			typeDomain = TypeCategory(typeOid);

			if (typeDomain == TYPCATEGORY_DATETIME)
				*is_numeric = false;
			else if  (typeDomain == TYPCATEGORY_NUMERIC) 
				*is_numeric = true;
			else {
				elog(ERROR, "unsupport range partition data type %d", 
						typeOid);
				return NULL;
			}

			initStringInfo(&namebuff);
			foreach(name_cell, colDef->typeName->names)
			{
				if (!first) {
					appendStringInfo(&namebuff,".");
				} else {
					first = false;
				}

				appendStringInfo(&namebuff, "%s", strVal(lfirst(name_cell)));
			}

			if (namebuff.len > 0) {
				typename = pstrdup(namebuff.data);
			}

			break;
		}
	}
	return typename;
}

/**
 * handle update partition key
 * */
static void handle_updatestmt(Query* parsetree)
{
	ListCell   *tl;
	char	*part_expr;
	Datum   values[Natts_table_partition_rule];
	bool    isnull[Natts_table_partition_rule];
	int     part_attr_no = InvalidAttrNumber;	

	RangeTblEntry *rte = rt_fetch(parsetree->resultRelation, parsetree->rtable);
	Relation rel = heap_open(rte->relid, AccessShareLock);
	char *relname = RelationGetRelationName(rel);
	char *schemaname = get_namespace_name(RelationGetNamespace(rel));

	if (!read_table_partition_rule_params(schemaname, relname, values, isnull)) {
		heap_close(rel, AccessShareLock);
		return ;
	} else {
		part_expr = TextDatumGetCString(values[Anum_table_partition_rule_cooked_expr-1]);
		part_attr_no = attnameAttNum(rel, part_expr, false);
		heap_close(rel, AccessShareLock);
	}

	foreach(tl, parsetree->targetList) {
		TargetEntry *tle = (TargetEntry *) lfirst(tl);
		if (tle->resno == part_attr_no) {
			elog(ERROR, " Cannot update partition attribute: %s of %s.%s", part_expr,
				 schemaname, relname);
		}
	}

	return ;
}

/**
 *after create table in standard_ProcessUtility, try to create children tables 
 * */
static void handle_createstmt(Node *parsetree, const char *queryString) 
{
	CreateStmt *stmt = (CreateStmt *)parsetree;
	RangeVar    *rv = stmt->relation;
	Datum   values[Natts_table_partition_rule];
	bool    isnull[Natts_table_partition_rule];
	StringInfoData sql;
	List *sql_list = NULL;
	char *remote_schema = NULL;
	Relation rel = heap_openrv(rv, AccessShareLock);
	Oid relid = RelationGetRelid(rel);
	char *ext_schemaname = get_namespace_name(get_pathman_schema());
    
	if (rv->schemaname == NULL)
		rv->schemaname = get_namespace_name(RelationGetNamespace(rel));

	heap_close(rel, AccessShareLock);

	if (!read_table_partition_rule_params(rv->schemaname, rv->relname, values, isnull))
		return ;

	if (!isnull[Anum_table_partition_rule_remote_schema-1]) {
		remote_schema = TextDatumGetCString(values[Anum_table_partition_rule_remote_schema-1]);
	} else {
		remote_schema = "public";
	}

	/*
 	 * call sql procedure (create_remote_hash_partitions/create_remote_range_partitions) 
 	 * to create patitions, which are foreign tables in fact
 	 * */
	initStringInfo(&sql);
	appendStringInfo(&sql, "select %s.", ext_schemaname);
	if (DatumGetUInt32(values[Anum_table_partition_rule_parttype -1]) == PT_HASH)
	{
		appendStringInfo(&sql, "create_remote_hash_partitions(");
		appendStringInfo(&sql, "%d,", relid);
		appendStringInfo(&sql, "'%s',", remote_schema);
		appendStringInfo(&sql, "'%s',", 
						 TextDatumGetCString(values[Anum_table_partition_rule_cooked_expr-1]));
		
	} else {
		bool is_numeric = false;
		char *typename = get_range_typename(stmt->tableElts, 
						TextDatumGetCString(values[Anum_table_partition_rule_cooked_expr-1]),
						&is_numeric);
		appendStringInfo(&sql, "create_remote_range_partitions(");
		appendStringInfo(&sql, "%d,", relid);
		appendStringInfo(&sql, "'%s',", remote_schema);
		appendStringInfo(&sql, "'%s',", 
						 TextDatumGetCString(values[Anum_table_partition_rule_cooked_expr-1]));
		appendStringInfo(&sql, "'%s'::%s,", 
						 TextDatumGetCString(values[Anum_table_partition_rule_range_start-1]),
						typename);
		if (is_numeric)
			appendStringInfo(&sql, "'%s'::%s,", 
						 	TextDatumGetCString(values[Anum_table_partition_rule_range_interval-1]),
							typename);
		else 
			appendStringInfo(&sql, "interval '%s',", 
						 	TextDatumGetCString(values[Anum_table_partition_rule_range_interval-1]));
		
		
	}
				
	appendStringInfo(&sql, "%d", 
					DatumGetUInt32(values[Anum_table_partition_rule_patitions_dist-1]));
	if (!isnull[Anum_table_partition_rule_server_list-1])
			appendStringInfo(&sql, ",'%s'", 
							 TextDatumGetCString(values[Anum_table_partition_rule_server_list-1]));


	appendStringInfo(&sql, ");");
	sql_list = list_make1(makeString(sql.data));
	spi_run_sql(sql_list);

	/**
 	* create tables on remote server via connections pool and modify foreign tables' options
 	*/
	{
		ListCell *meta_cell;
		char *tmp_start, *tmp_end;
		List *meta_list = get_remote_meta_4_child_foreign_table(rv);
		/* Create remote table */
		tmp_start = strcasestr(queryString, "table");
		tmp_start += 5;
		if (stmt->if_not_exists) {
			tmp_start = strcasestr(tmp_start, "exists");
			tmp_start += 6;
		}

		tmp_end = tmp_start;
		while ((*tmp_end != '(') &&
				 (tmp_end < (queryString + strlen(queryString))))
		{
			tmp_end++;
		}

		foreach(meta_cell, meta_list) {
			Remote_meta *meta = lfirst(meta_cell);
			resetStringInfo(&sql);
			resetStringInfo(&sql);
			appendBinaryStringInfo(&sql, queryString, tmp_start-queryString);
			appendStringInfo(&sql, " %s.%s ", meta->remote_schema, meta->remote_table);
			appendBinaryStringInfo(&sql, tmp_end, queryString+strlen(queryString)-tmp_end);
			connectionPoolRunSQL(meta->user, sql.data, true);
		}

	}

}

/**
 *	handle truncate table before hook, set rangeval to no inh if target table
 * */
static void handle_truncatestmt_before_hook(Node *parsetree)
{
	ListCell   *cell;
	TruncateStmt *stmt = (TruncateStmt *) parsetree;

	foreach (cell, stmt->relations)
	{
		RangeVar   *rv = lfirst(cell);

		if (rv->schemaname == NULL) {
			Relation rel = heap_openrv(rv, AccessShareLock);
			rv->schemaname = get_namespace_name(RelationGetNamespace(rel));
			heap_close(rel, AccessShareLock);
		}

#if PG_VERSION_NUM >= 100000
		if (read_table_partition_rule_params(rv->schemaname, rv->relname, NULL, NULL) &&
				rv->inh) 
		{
			rv->inh = false;
		}
#else 
		if (read_table_partition_rule_params(rv->schemaname, rv->relname, NULL, NULL) &&
				rv->inhOpt != INH_NO) 
		{
			rv->inhOpt = INH_NO;
		}
#endif
	}

}
/**
 *	try to truncate only table on remote children table
 * */
static void handle_truncatestmt(Node *parsetree)
{
	ListCell   *cell;
	TruncateStmt *stmt = (TruncateStmt *) parsetree;

	foreach (cell, stmt->relations)
	{
		RangeVar   *rv = lfirst(cell);

		if (rv->schemaname == NULL) {
			Relation rel = heap_openrv(rv, AccessShareLock);
			rv->schemaname = get_namespace_name(RelationGetNamespace(rel));
			heap_close(rel, AccessShareLock);
		}

		if (read_table_partition_rule_params(rv->schemaname, rv->relname, NULL, NULL)) {
			ListCell* meta_cell;
			StringInfoData sql;
	
			List *remote_meta_list = get_remote_meta_4_child_foreign_table(rv);
			initStringInfo(&sql);

			foreach(meta_cell, remote_meta_list) 
			{
				Remote_meta *meta = lfirst(meta_cell);
				appendStringInfo(&sql, "truncate table %s.%s", 
								 meta->remote_schema, meta->remote_table);

				if (stmt->behavior == DROP_CASCADE)
					appendStringInfo(&sql, " cascade"); 

				elog(DEBUG3, " Try to run %s on %d", sql.data, meta->user->serverid);
				connectionPoolRunSQL(meta->user, sql.data, true);
				resetStringInfo(&sql);
			}
		}
	}
}

/*
 *	Try to create index on remote children table
 * */
static void handle_create_index(Node *parsetree, const char* queryString)
{
	IndexStmt  *stmt = (IndexStmt *) parsetree;
	RangeVar    *rv = stmt->relation;
	char		*schemaname = rv->schemaname;

	if (schemaname == NULL) {
		Relation rel = heap_openrv(rv, AccessShareLock);
		schemaname = get_namespace_name(RelationGetNamespace(rel));
		heap_close(rel, AccessShareLock);
	}
					
	if (read_table_partition_rule_params(schemaname, rv->relname,
										 NULL, NULL)) {
		ListCell *meta_cell;
		StringInfoData sql_head, create_index_sql;
		const char *body = queryString + rv->location;
		List *remote_meta_list =  get_remote_meta_4_child_foreign_table(rv);

		initStringInfo(&sql_head);
		initStringInfo(&create_index_sql);
		appendStringInfo(&sql_head, "CREATE ");
		if (stmt->unique)
			appendStringInfo(&sql_head, "UNIQUE ");

		appendStringInfo(&sql_head, "INDEX ");

		if (stmt->concurrent)
			appendStringInfo(&sql_head, "CONCURRENTLY ");

		if (strcmp(stmt->accessMethod, "btree") != 0)
			body = strcasestr(body, "using");
		else {
			while (*body != '(' && body < queryString + strlen(queryString))
				body++;
		} 
			
		foreach(meta_cell, remote_meta_list)
		{
			Remote_meta *meta = lfirst(meta_cell);
			/*
 			 * name index on remote table as "remote_table+_+indexname"
 			 * */
			appendStringInfo(&create_index_sql, "%s %s_%s on %s.%s %s", sql_head.data, 
								meta->remote_table, stmt->idxname, meta->remote_schema,
								meta->remote_table, body);
			elog(DEBUG3, " Try to run  %s on %d", create_index_sql.data, meta->user->serverid);
			connectionPoolRunSQL(meta->user, create_index_sql.data, true);
			resetStringInfo(&create_index_sql);
		}
	}
}

/**
 *	Try to handle drop stmt before standard_ProcessUtility
 * */
static void handle_dropstmt(Node   *parsetree)
{
	DropStmt   *stmt = (DropStmt *) parsetree;
	if (stmt->removeType == OBJECT_INDEX) {

		ListCell   *cell;
		RangeVar 	*rv, *indexed_rv;
		Relation 	indexed_rel;

		foreach(cell, stmt->objects)
		{

			rv = makeRangeVarFromNameList((List *) lfirst(cell));
			indexed_rel = relation_openrv_extended(rv, AccessShareLock,
								stmt->missing_ok);
			if (stmt->missing_ok && !indexed_rel)
					continue ;

			indexed_rv = makeRangeVar(get_namespace_name(RelationGetNamespace(indexed_rel)),
										 	RelationGetRelationName(indexed_rel), -1);
			heap_close(indexed_rel, AccessShareLock);
							
			if (read_table_partition_rule_params(indexed_rv->schemaname, indexed_rv->relname,
												NULL, NULL)) {
				List *remote_meta_list = get_remote_meta_4_child_foreign_table(indexed_rv);
				ListCell *meta_cell;
				StringInfoData drop_index_sql;
				initStringInfo(&drop_index_sql);

				foreach(meta_cell, remote_meta_list)
				{
					Remote_meta *meta = lfirst(meta_cell);

					appendStringInfo(&drop_index_sql, "DROP INDEX ");
					if (stmt->missing_ok)
						appendStringInfo(&drop_index_sql, "IF EXISTS ");

					appendStringInfo(&drop_index_sql, "%s.%s_%s ", meta->remote_schema,
							meta->remote_table, rv->relname);
					if (stmt->behavior == DROP_CASCADE)
						appendStringInfo(&drop_index_sql, "CASCADE");

					appendStringInfo(&drop_index_sql, ";");
					elog(DEBUG3, " Try to run %s on %d", drop_index_sql.data, meta->user->serverid);
					connectionPoolRunSQL(meta->user, drop_index_sql.data, true);
					resetStringInfo(&drop_index_sql);
				}
			}
		}
	}
	else if (stmt->removeType == OBJECT_FOREIGN_TABLE) {
		ListCell  *cell;
		RangeVar  *rv,*parent_rv;
		foreach(cell, stmt->objects)
		{
			rv = makeRangeVarFromNameList((List *) lfirst(cell));
			parent_rv = get_parent_rangevar(rv);
			if (parent_rv) {
				if (parent_rv->schemaname == NULL) {
					Relation rel = heap_openrv(rv, AccessShareLock);
					parent_rv->schemaname = get_namespace_name(RelationGetNamespace(rel));
					heap_close(rel, AccessShareLock);
				}

 				if (read_table_partition_rule_params(parent_rv->schemaname,
													parent_rv->relname, NULL, NULL)) {
					if (rv->schemaname == NULL) {
						Relation childrel = heap_openrv(rv, AccessShareLock);
						rv->schemaname = get_namespace_name(RelationGetNamespace(childrel));
						heap_close(childrel, AccessShareLock);
					}
							
					elog(ERROR, " Cannot drop table %s.%s because %s.%s depends on it",
							rv->schemaname, rv->relname, parent_rv->schemaname, parent_rv->relname);
				}
			}	
		}
	} else if (stmt->removeType == OBJECT_TABLE) {
		ListCell   *cell;
		RangeVar   *parent_rv;
		List	   *target_list = NULL;
		foreach(cell, stmt->objects)
		{
			parent_rv = makeRangeVarFromNameList((List *) lfirst(cell));
			if (!parent_rv->schemaname) {
				Relation rel = relation_openrv_extended(parent_rv, AccessShareLock,
									stmt->missing_ok);
				if (stmt->missing_ok && !rel)
					continue ;

				parent_rv->schemaname = get_namespace_name(RelationGetNamespace(rel));
				heap_close(rel, AccessShareLock);
			}

			if (parent_rv && read_table_partition_rule_params(parent_rv->schemaname, 
														 		parent_rv->relname, 
														 		NULL, NULL)) {
				target_list = lappend(target_list, parent_rv);
			}
		}

		if (target_list == NULL)
			return ;

		if ( list_length(target_list) != list_length(stmt->objects)) {
			if (stmt->behavior != DROP_CASCADE) {
				elog(ERROR, "Cannot drop table without CASCADE, "
					"because some table in partition table rule");
				return ;
			}
		} else {
			if (stmt->behavior != DROP_CASCADE) {
				stmt->behavior = DROP_CASCADE;
			}
		}

		foreach(cell, target_list) 
		{
			StringInfoData 	sql;
			ListCell	*meta_cell;
			List 		*remote_meta_list;
			parent_rv = lfirst(cell);
			remote_meta_list = get_remote_meta_4_child_foreign_table(parent_rv);
			initStringInfo(&sql);

			foreach(meta_cell, remote_meta_list) {
				Remote_meta *meta = lfirst(meta_cell);
				appendStringInfo(&sql, "drop table ");
				if (stmt->missing_ok)
					appendStringInfo(&sql, "if exists ");

				appendStringInfo(&sql, "%s.%s;", meta->remote_schema, meta->remote_table);
				elog(DEBUG3, " Try to run drop table %s.%s on %d", meta->remote_schema,
							 meta->remote_table, meta->user->serverid );
				connectionPoolRunSQL(meta->user, sql.data, true);
				resetStringInfo(&sql);
			}

		}	
	}
}

/**
 *	Try to cluster on remote children table
 * */
static void handle_clusterstmt(Node *parsetree)
{
	ClusterStmt *stmt = (ClusterStmt *) parsetree;
	RangeVar    *rv = stmt->relation;
	char		*schemaname = rv->schemaname;

	if (rv == NULL)
		return ;

	if (schemaname == NULL) {
		Relation rel = heap_openrv(rv, AccessShareLock);
		schemaname = get_namespace_name(RelationGetNamespace(rel));
		heap_close(rel, AccessShareLock);
	}

	if (read_table_partition_rule_params(schemaname, rv->relname, NULL, NULL)) {
		List *remote_meta_list =  get_remote_meta_4_child_foreign_table(rv);
		ListCell *meta_cell;
		StringInfoData cluster_sql;
		initStringInfo(&cluster_sql);

		foreach(meta_cell, remote_meta_list) 
		{
			Remote_meta *meta = lfirst(meta_cell);
			appendStringInfo(&cluster_sql, "CLUSTER %s.%s ", 
							meta->remote_schema, meta->remote_table);

			if (stmt->indexname)
				appendStringInfo(&cluster_sql, "USING %s_%s", 
							meta->remote_table, stmt->indexname);

			appendStringInfo(&cluster_sql, ";"),
			connectionPoolRunSQL(meta->user, cluster_sql.data, true);
			elog(DEBUG3, " Try to run %s on %d", cluster_sql.data,
				 meta->user->serverid);

			resetStringInfo(&cluster_sql);
		}
	}

}

/**
 *	Try to vacuum remote children table
 *
 * */
static void handle_vacuumstmt(Node *parsetree, const char* queryString)
{
	RangeVar   *rv = NULL;
	char	   *schemaname = NULL;
	VacuumStmt *stmt = (VacuumStmt *) parsetree;
#if PG_VERSION_NUM >= 110000
	if (list_length(stmt->rels) != 1) {
		ListCell *rv_cell = NULL;
		foreach(rv_cell, stmt->rels)
		{
			rv = (RangeVar *) lfirst(rv_cell);
			schemaname = rv->schemaname;

			if (schemaname == NULL) {
				Relation rel = heap_openrv(rv, AccessShareLock);
				schemaname = get_namespace_name(RelationGetNamespace(rel));
				heap_close(rel, AccessShareLock);
			}

			if (read_table_partition_rule_params(schemaname, rv->relname,
												NULL, NULL)) {
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Vacuum multiple tables in table_partition_rule not supported"),
						errhint("Vacuum table one by one instead.")));	
			}
		}
		return;
	}

	rv = (RangeVar *)lfirst(list_head(stmt->rels));
	return;
#else 
	rv = stmt->relation;
#endif

	if (rv == NULL)
		return ;

	schemaname = rv->schemaname;

	if (schemaname == NULL) {
		Relation rel = heap_openrv(rv, AccessShareLock);
		schemaname = get_namespace_name(RelationGetNamespace(rel));
		heap_close(rel, AccessShareLock);
	}

	if (read_table_partition_rule_params(schemaname, rv->relname, NULL, NULL)) {
		List 		*remote_meta_list = NIL;
		ListCell 	*meta_cell = NULL;
		const char	*tail_start = NULL;

		StringInfoData vacuum_head_sql, vacuum_sql;
		initStringInfo(&vacuum_head_sql);
		initStringInfo(&vacuum_sql);
		appendBinaryStringInfo(&vacuum_head_sql, queryString, rv->location);

		tail_start = queryString + rv->location;
		while (*tail_start != ' ' && tail_start <= queryString+rv->location)
			tail_start++;

		remote_meta_list = get_remote_meta_4_child_foreign_table(rv);
		foreach(meta_cell, remote_meta_list) 
		{
			Remote_meta *meta = lfirst(meta_cell);

			appendStringInfo(&vacuum_sql, " %s %s.%s ",  vacuum_head_sql.data, 
								meta->remote_schema, meta->remote_table);

			if (tail_start < (queryString+rv->location))
				appendStringInfo(&vacuum_sql, " %s ", tail_start);
			else 
				appendStringInfo(&vacuum_sql, ";");

			elog(DEBUG3, " Try to run %s on %d", vacuum_sql.data,
				 meta->user->serverid);

			connectionPoolRunSQL(meta->user, vacuum_sql.data, false);
			resetStringInfo(&vacuum_sql);
		}
	}

}

/**
 *	Reindex on remote children table
 * */
static void handle_reindexstmt(Node* parsetree, const char* queryString)
{
	ReindexStmt *stmt = (ReindexStmt *) parsetree;
	RangeVar    *rv = stmt->relation;
	if (stmt->kind == REINDEX_OBJECT_INDEX) {
		RangeVar *indexed_rv;
		Relation indexed_rel;
		Oid		indexed_relid;
		indexed_relid = IndexGetRelation(RangeVarGetRelid(rv, AccessShareLock, false), false);

		indexed_rel = heap_open(indexed_relid, AccessShareLock);
		indexed_rv = makeRangeVar(get_namespace_name(RelationGetNamespace(indexed_rel)),
										 	RelationGetRelationName(indexed_rel), -1);
		heap_close(indexed_rel, AccessShareLock);

		if (read_table_partition_rule_params(indexed_rv->schemaname, 
											 indexed_rv->relname, NULL, NULL)) {
			ListCell *meta_cell;
			StringInfoData reindex_sql;

			List *remote_meta_list =  get_remote_meta_4_child_foreign_table(indexed_rv);
			initStringInfo(&reindex_sql);
			foreach(meta_cell, remote_meta_list) 
			{
				Remote_meta *meta = lfirst(meta_cell);
				if (stmt->options == 0)
					appendStringInfo(&reindex_sql, "REINDEX index");
				else {
					appendBinaryStringInfo(&reindex_sql, queryString, rv->location);
				}

				appendStringInfo(&reindex_sql, " %s.%s_%s;", meta->remote_schema, 
								meta->remote_table, rv->relname);
				connectionPoolRunSQL(meta->user, reindex_sql.data, true);
				elog(DEBUG3, " Try to run %s on %d", reindex_sql.data,  meta->user->serverid);
				resetStringInfo(&reindex_sql);
			}
		}
	
	} else if (stmt->kind == REINDEX_OBJECT_TABLE) {
		char		*schemaname = rv->schemaname;
		if (schemaname == NULL) {
			Relation rel = heap_openrv(rv, AccessShareLock);
			schemaname = get_namespace_name(RelationGetNamespace(rel));
			heap_close(rel, AccessShareLock);
		}
		
		if (read_table_partition_rule_params(schemaname, rv->relname, NULL, NULL)) {
			ListCell *meta_cell;
			List *remote_meta_list =  get_remote_meta_4_child_foreign_table(rv);
			StringInfoData reindex_sql;
			initStringInfo(&reindex_sql);
			foreach(meta_cell, remote_meta_list) 
			{
				Remote_meta *meta = lfirst(meta_cell);
				if (stmt->options == 0)
					appendStringInfo(&reindex_sql, "REINDEX table");
				else {
					appendBinaryStringInfo(&reindex_sql, queryString, rv->location);
				}
				appendStringInfo(&reindex_sql, " %s.%s;", meta->remote_schema, meta->remote_table);
				connectionPoolRunSQL(meta->user, reindex_sql.data, true);
				elog(DEBUG3, " Try to run %s on %d", reindex_sql.data,  meta->user->serverid);
				resetStringInfo(&reindex_sql);
			}
		}
	}
}

/**
 * handle rename before standard_ProcessUtility
 * */
static void handle_renamestmt(Node* parsetree)
{
	RenameStmt *stmt = (RenameStmt *) parsetree;
	switch (stmt->renameType) {
		case OBJECT_INDEX:
		{
			RangeVar *indexed_rv;
			Relation indexed_rel;
			Oid		indexed_relid;

			RangeVar    *rv = stmt->relation;
			indexed_relid = IndexGetRelation(RangeVarGetRelid(rv, AccessShareLock, false), false);

			indexed_rel = heap_open(indexed_relid, AccessShareLock);
			indexed_rv = makeRangeVar(get_namespace_name(RelationGetNamespace(indexed_rel)),
										 	RelationGetRelationName(indexed_rel), -1);
			heap_close(indexed_rel, AccessShareLock);

			if (read_table_partition_rule_params(indexed_rv->schemaname, indexed_rv->relname,
												 NULL, NULL)) {
				elog(ERROR, "Cannot rename index %s because it is used by partition table",
					 rv->relname);
			}
		}
			break;

		case OBJECT_COLUMN:
		case OBJECT_ATTRIBUTE:
		case OBJECT_TABLE:
		{
			RangeVar    *rv = stmt->relation;
			char *schemaname = NULL;
			bool is_foreign_table = false;
			Relation rel = heap_openrv(rv, AccessShareLock);
			is_foreign_table = (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE);
			if (!is_foreign_table) {
				if (rv->schemaname)
					schemaname = rv->schemaname;
				else 	
					schemaname = get_namespace_name(RelationGetNamespace(rel));
			}
			heap_close(rel, AccessShareLock);
				
			if (!is_foreign_table) {
				if (read_table_partition_rule_params(schemaname, rv->relname, NULL, NULL)) {
					if (stmt->renameType == OBJECT_TABLE)
						elog(ERROR, "Cannot rename table %s.%s because it is in table partition rule",
							schemaname, rv->relname);
					else 
						elog(ERROR, "Cannot rename column %s of %s.%s because table in table partition rule",
							stmt->subname, schemaname, rv->relname);				
				}
			} else {
				RangeVar    *parent_rv = get_parent_rangevar(rv);
				schemaname = parent_rv->schemaname;
				if (parent_rv->schemaname == NULL) {
					Relation parent_rel = heap_openrv(parent_rv, AccessShareLock);
					schemaname = get_namespace_name(RelationGetNamespace(parent_rel));
					heap_close(parent_rel, AccessShareLock);
				}

				if (read_table_partition_rule_params(schemaname, parent_rv->relname, NULL, NULL)) {
					if (stmt->renameType == OBJECT_TABLE)
						elog(ERROR, "Cannot rename foreign table %s because it is depended by %s.%s",
							rv->relname, schemaname, parent_rv->relname);
					else 
						elog(ERROR, "Cannot rename column %s of %s "
							"because its parent table %s.%s in table partition rule",
							stmt->subname, rv->relname, schemaname, parent_rv->relname);
				}

			}
		}
			break;

		case OBJECT_FOREIGN_TABLE:
		{
			RangeVar    *rv = stmt->relation;
			RangeVar    *parent_rv = get_parent_rangevar(rv);
			char *schemaname = parent_rv->schemaname;
			if (parent_rv->schemaname == NULL) {
				Relation rel = heap_openrv(rv, AccessShareLock);
				schemaname = get_namespace_name(RelationGetNamespace(rel));
				heap_close(rel, AccessShareLock);
			}
			
			if (read_table_partition_rule_params(schemaname, parent_rv->relname, NULL, NULL)) {
				elog(ERROR, "Cannot rename foreign table %s because it is depended by %s.%s",
					rv->relname, schemaname, parent_rv->relname);
			}

		}
			break;

		case OBJECT_SCHEMA:
		{
			if (schema_in_table_partition_rule(stmt->subname)) {
				elog(ERROR, "Cannot rename schema %s because it is used by table_partition_rule",
					stmt->subname);
			} else if (strcasecmp(stmt->subname, PARTITION_TABLE_SCHEMA) == 0) {
				elog(ERROR, "Cannot rename gogudb system schema %s ", stmt->subname);
			}
		}
			break;

		default:
			break;
	}

}

/*
 *	Try to alter remote children table
 * */
static void handle_altertable_stmt(Node* parsetree, const char *queryString)
{
	AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
	RangeVar    *rv = stmt->relation;
	List 		*remote_meta_list = NULL;
	ListCell 	*meta_cell = NULL;
	const char 	*tail_start = NULL;
	StringInfoData altertable_sql;

	if (stmt->relkind == OBJECT_TABLE) {
		char *schemaname = rv->schemaname;
		if (schemaname == NULL) {
			Relation rel = heap_openrv(rv, AccessShareLock);
			schemaname = get_namespace_name(RelationGetNamespace(rel));
			heap_close(rel, AccessShareLock);
		}
	
		if (read_table_partition_rule_params(schemaname, rv->relname, NULL, NULL)) {
			remote_meta_list =  get_remote_meta_4_child_foreign_table(rv);
		} else {
			return ;
		}
	} else if (stmt->relkind == OBJECT_INDEX) {
		RangeVar *indexed_rv;
		Relation rel,	indexed_rel;
		Oid		indexed_relid;

		rel = heap_openrv(rv, AccessShareLock);
		indexed_relid = IndexGetRelation(RelationGetRelid(rel), false);
		heap_close(rel, AccessShareLock);

		indexed_rel = heap_open(indexed_relid, AccessShareLock);
		indexed_rv = makeRangeVar(get_namespace_name(RelationGetNamespace(indexed_rel)),
										 	RelationGetRelationName(indexed_rel), -1);
		heap_close(indexed_rel, AccessShareLock);

		if (read_table_partition_rule_params(indexed_rv->schemaname, 
											 indexed_rv->relname,
											 NULL, NULL)) {
			remote_meta_list =  get_remote_meta_4_child_foreign_table(indexed_rv);
		} else {
			return ;
		}
	} else {
		return ;
	}

	tail_start = queryString + rv->location;
	while (*tail_start != ' ' && 
			tail_start <= queryString+ strlen(queryString))
		tail_start++;

	initStringInfo(&altertable_sql);
	foreach(meta_cell, remote_meta_list) 
	{
		Remote_meta *meta = lfirst(meta_cell);
		appendBinaryStringInfo(&altertable_sql, queryString, rv->location);
		if (stmt->relkind == OBJECT_TABLE) {
			appendStringInfo(&altertable_sql, " %s.%s ", meta->remote_schema,
									 meta->remote_table);
		}
		else {
			appendStringInfo(&altertable_sql, " %s_%s ", meta->remote_table,
							 rv->relname);
		}

		appendStringInfo(&altertable_sql, "%s ", tail_start);
		connectionPoolRunSQL(meta->user, altertable_sql.data, true);
		elog(DEBUG3, " Try to run %s on %d", altertable_sql.data,  meta->user->serverid);
		resetStringInfo(&altertable_sql);
	}
}

static void handle_after_hook(Node *parsetree, const char *queryString)
{
	/* handle remote partitions */

	switch (nodeTag(parsetree))
	{
		case T_TruncateStmt:
			{
				handle_truncatestmt(parsetree);
			}
			break;

		case T_ClusterStmt:
			{
				handle_clusterstmt(parsetree);
			}
			break;

		case T_VacuumStmt: 
			{
				handle_vacuumstmt(parsetree, queryString);
			}
			break;

		case T_ReindexStmt:
			{
				handle_reindexstmt(parsetree, queryString);
			}
			break;

		case T_CreateStmt:
			{
				handle_createstmt(parsetree, queryString);
			}
			break;

		case T_AlterTableStmt:
			{

				handle_altertable_stmt(parsetree, queryString);
			}
			break;

		case T_IndexStmt:
			{
				handle_create_index(parsetree, queryString);
			}
			break;

		default:
			break;
	}	
}

List* pg_plan_queries_hook(List *querytrees, int cursorOptions,
							ParamListInfo boundParams)
{
	Query			*parse;
	RangeTblEntry	*parent_rte;
	Relation 		parent_rel;
	char 			*parent_relname,
					*parent_schemaname; 
	const PartRelationInfo *prel;
	FromExpr  		*jtnode;
	Node			*qual_expr;
	List			*ranges, *wrappers, *quals;	
	WalkerContext	context;
	Node		  	*part_expr;
	//List		   	*part_clauses;
	ListCell	   	*lc;
	Oid            	*children, child_oid;
	IndexRange 		irange;
	PlannedStmt		*plan_stmt;

	if (!IsPathmanReady() || (list_length(querytrees)>1))
		return NULL;

	parse = lfirst_node(Query, list_head(querytrees)); 

	if (!((parse->commandType == CMD_SELECT) && parse->rtable &&
            (list_length(parse->rtable)==1) && !(parse->hasSubLinks)))
		return NULL;

	jtnode = (FromExpr *)(parse->jointree);
	if (jtnode == NULL || !IsA(jtnode, FromExpr))
		return NULL;

	
	parent_rte = lfirst_node(RangeTblEntry, list_head(parse->rtable));
	if (parent_rte->rtekind != RTE_RELATION)
		return NULL;

	parent_rel = heap_open(parent_rte->relid, AccessShareLock);
	parent_relname = RelationGetRelationName(parent_rel);
	parent_schemaname = get_namespace_name(RelationGetNamespace(parent_rel));
	heap_close(parent_rel, AccessShareLock);   

	if (!read_table_partition_rule_params(parent_schemaname, parent_relname, NULL, NULL))
		return NULL;

	prel = get_pathman_relation_info(parent_rte->relid);
	if (prel == NULL)
		return NULL;

	children = PrelGetChildrenArray(prel);
	
	qual_expr = ((jtnode))->quals;
	if (qual_expr == NULL)
		return NULL;

	qual_expr = eval_const_expressions(NULL, qual_expr);
	qual_expr = (Node *) canonicalize_qual((Expr *) qual_expr
#if PG_VERSION_NUM >= 110000
											,false
#endif
											);
	quals = make_ands_implicit((Expr *) qual_expr);

	ranges = list_make1_irange_full(prel, IR_COMPLETE);

	part_expr = PrelExpressionForRelid(prel, 1);
	/*
 	 * part_clauses = get_partitioning_clauses(quals, prel, 1);
	if (clause_contains_params((Node *) part_clauses))
		return NULL;
	TBD LATER */

	/* Make wrappers over restrictions and collect final rangeset */
	InitWalkerContext(&context, part_expr, prel, NULL);
	wrappers = NIL;
	foreach(lc, quals)
	{
		WrapperNode	   *wrap;
		Node   *qual_node = (Node *) lfirst(lc);

		wrap = walk_expr_tree((Expr*)qual_node, &context);
		wrappers = lappend(wrappers, wrap);
		ranges = irange_list_intersection(ranges, wrap->rangeset);
	}

	if (irange_list_length(ranges) != 1)
		return NULL;

	irange = lfirst_irange(list_head(ranges));
	if (irange_lower(irange) != irange_upper(irange))
		return NULL;

	child_oid = children[irange_lower(irange)];
	parent_rte->relid = child_oid;
	parent_rte->rtekind |= CMD_SPECIAL_BIT;
	
	plan_stmt = makeNode(PlannedStmt);
	plan_stmt->commandType = CMD_SELECT;
	plan_stmt->queryId = parse->queryId;
	plan_stmt->dependsOnRole = false;
	plan_stmt->planTree = (Plan*)make_foreignscan(parse->targetList, NULL, child_oid, NULL,
												NULL, NULL, NULL, NULL);
	plan_stmt->rtable = parse->rtable;
	plan_stmt->resultRelations = NULL;
	plan_stmt->subplans = NULL;
	plan_stmt->canSetTag = true;
	return list_make1(plan_stmt);
}

/*
 * try to send sql to server and build tupdesc
 * */
bool PortalStart_hook(Portal portal, ParamListInfo params,
                                int eflags, Snapshot snapshot)
{

	PlannedStmt		*plan;
	RangeTblEntry	*rte;

	if (portal->stmts == NIL )
		return false;

	plan = lfirst_node(PlannedStmt, list_head(portal->stmts)); 
	
	if (plan->commandType != CMD_SELECT)
		return false;

	if (plan->rtable == NIL)
		return false;

	rte = lfirst_node(RangeTblEntry, list_head(plan->rtable));
	if (!is_cmd_special(rte->rtekind))
		return false;
 
	portal->tupDesc = ExecTypeFromTL(plan->planTree->targetlist, false);
	portal->atStart = true;
	portal->atEnd = false;  /* allow fetches */
	portal->portalPos = 0;
	portal->status = PORTAL_READY;
	return true;
}

bool PortalRun_hook(Portal portal, long count, bool isTopLevel,
#if PG_VERSION_NUM >= 100000
					bool run_once,
#endif
					DestReceiver *dest,
					DestReceiver *altdest, char *completionTag)
{
	const char		*child_relname = NULL;
	const char		*child_schename = NULL;
	const char 		*sql = NULL;
	char			*index;
	StringInfoData	remote_sql;
	Oid				userid;
	UserMapping 	*user;
	PGconn			*conn;
	PGresult		*cur_res;

	ForeignTable 	*ftable;
	PlannedStmt		*plan;
	RangeTblEntry	*rte;
	HeapTuple       tuple; 
	TupleDesc       tupdesc;
	char			**values;
	AttInMetadata 	*attinmeta;
	TupleTableSlot	*slot;
	int				i;
	ListCell	   	*lc;

	if (!portal->stmts)
		return false;

	plan = lfirst_node(PlannedStmt, list_head(portal->stmts)); 
	
	if (plan->rtable == NIL)
		return false;

	if (plan->commandType != CMD_SELECT)
		return false;

	rte = lfirst_node(RangeTblEntry, list_head(plan->rtable));
	if (!is_cmd_special(rte->rtekind))
		return false;

	portal->status = PORTAL_ACTIVE;
	slot = MakeTupleTableSlot(
#if PG_VERSION_NUM >= 110000
							  NULL
#endif	
							  );
	tupdesc = portal->tupDesc;
	ExecSetSlotDescriptor(slot, tupdesc);
	(*dest->rStartup) (dest, CMD_SELECT, tupdesc);
	ftable = GetForeignTable(rte->relid);
	if (ftable == NULL) {
		elog( ERROR, "FATAL when handle %s", portal->sourceText);
		return false;
	}

	foreach(lc, ftable->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "schema_name") == 0)
			child_schename = defGetString(def);
		else if (strcmp(def->defname, "table_name") == 0)
			child_relname = defGetString(def);
	}

	if (child_relname == NULL) {
		elog( ERROR, "Failed to get table_name option for %d", rte->relid);
		return false;
	}

	if (child_schename == NULL)
		child_schename = "public";

	// OK, to replace the table name, later use get_query_def instead
	sql = portal->sourceText;
	index = strstr(sql, rte->eref->aliasname);
	initStringInfo(&remote_sql);
	appendBinaryStringInfo(&remote_sql, sql, index-sql);
	appendStringInfo(&remote_sql, " %s.%s ", child_schename, child_relname);
	index += strlen(rte->eref->aliasname);
	appendBinaryStringInfo(&remote_sql, index, sql+strlen(sql)-index);

	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
	user = GetUserMapping(userid, ftable->serverid);
	conn = GoguGetConnection(user, false, false);
	if (!PQsendQuery(conn, remote_sql.data)) {
		elog(ERROR, "Failed to run SQL %s", remote_sql.data);
		return false;
	}

	if (!PQsetSingleRowMode(conn)) {
		elog(ERROR, "Failed to set single row mode for %s", remote_sql.data);
		return false;
	}

	attinmeta = TupleDescGetAttInMetadata(tupdesc);
	values = (char**) palloc0(tupdesc->natts * sizeof(char*));

	for(; ;) {
		CHECK_FOR_INTERRUPTS();
		cur_res = PQgetResult(conn);
		if (PQresultStatus(cur_res) == PGRES_TUPLES_OK)
		{
			PQclear(cur_res);
			do {
					cur_res = PQgetResult(conn);
			} while (cur_res != NULL);

			break;
		}

		if (PQresultStatus(cur_res) != PGRES_SINGLE_TUPLE)
		{
			elog(ERROR, "Failed to fetch row for %s", remote_sql.data);
			return false;
		}

		memset(values, 0, tupdesc->natts * sizeof(char*));

		for (i=0; i < tupdesc->natts; i++) 
		{
			if (PQgetisnull(cur_res, 0, i))
				values[i] = NULL;
			else
				values[i] = PQgetvalue(cur_res, 0, i);	
		}

		tuple = BuildTupleFromCStrings(attinmeta, values);
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);

		if (!((*dest->receiveSlot) (slot, dest)))
			break;

	}

	GoguReleaseConnection(conn);
	(*dest->rShutdown) (dest);
	portal->status = PORTAL_READY;
	return true;
}
