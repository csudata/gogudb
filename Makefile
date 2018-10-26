# contrib/pg_pathman/Makefile

MODULE_big = gogudb

OBJS = src/init.o src/relation_info.o src/utils.o src/partition_filter.o \
	src/runtimeappend.o src/runtime_merge_append.o src/pg_pathman.o src/rangeset.o \
	src/pl_funcs.o src/pl_range_funcs.o src/pl_hash_funcs.o src/pathman_workers.o \
	src/hooks.o src/nodes_common.o src/xact_handling.o src/utility_stmt_hooking.o \
	src/planner_tree_modification.o src/debug_print.o src/partition_creation.o \
	src/compat/pg_compat.o src/compat/rowmarks_fix.o \
	src/postgres_fdw${MAJORVERSION}.o src/option.o src/deparse${MAJORVERSION}.o \
	src/connection.o src/shippable.o src/hot_patch.o src/libudis86/decode.o	\
	src/libudis86/itab.o src/libudis86/syn-att.o src/libudis86/syn.o \
	src/libudis86/syn-intel.o src/libudis86/udis86.o \
	$(WIN32RES)

SHLIB_LINK = $(libpq)

EXTENSION = gogudb

EXTVERSION = 1.1

DATA_built = $(EXTENSION)--$(EXTVERSION).sql

PGFILEDESC = "gogudb - database cluster of PostgreSQL"

REGRESS = pathman_array_qual \
		  pathman_basic \
		  pathman_bgw \
		  pathman_calamity \
		  pathman_callbacks \
		  pathman_column_type \
		  pathman_cte \
		  pathman_domains \
		  pathman_dropped_cols \
		  pathman_expressions \
		  pathman_foreign_keys \
		  pathman_gaps \
		  pathman_inserts \
		  pathman_interval \
		  pathman_join_clause \
		  pathman_lateral \
		  pathman_mergejoin \
		  pathman_only \
		  pathman_param_upd_del \
		  pathman_permissions \
		  pathman_rebuild_updates \
		  pathman_rowmarks \
		  pathman_runtime_nodes \
		  pathman_update_trigger \
		  pathman_upd_del \
		  pathman_utility_stmt \
		  pathman_views \
		  gogudb_basic${MAJORVERSION} \
		  gogudb_fdw${MAJORVERSION}

EXTRA_REGRESS_OPTS=--temp-config=$(top_srcdir)/$(subdir)/conf.add
EXTRA_CLEAN = $(EXTENSION)--$(EXTVERSION).sql ./isolation_output

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
override PG_CPPFLAGS += -I$(CURDIR)/src/include -I$(shell $(PG_CONFIG) --includedir)
include $(PGXS)
else
override PG_CPPFLAGS += -I$(top_srcdir)/$(subdir)/src/include -I$(libpq_srcdir)
subdir = contrib/gogudb
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

$(EXTENSION)--$(EXTVERSION).sql: init.sql hash.sql range.sql
	cat $^ > $@

ISOLATIONCHECKS=insert_nodes for_update rollback_on_create_partitions

submake-isolation:
	$(MAKE) -C $(top_builddir)/src/test/isolation all

isolationcheck: | submake-isolation
	$(MKDIR_P) isolation_output
	$(pg_isolation_regress_check) \
		--temp-config=$(top_srcdir)/$(subdir)/conf.add \
		--outputdir=./isolation_output \
		$(ISOLATIONCHECKS)

python_tests:
	$(MAKE) -C tests/python partitioning_tests

cmocka_tests:
	$(MAKE) -C tests/cmocka check
