/*-------------------------------------------------------------------------
 *
 * hba_fdw.c
 *        foreign-data wrapper for pg_hba.conf files.
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        hba_fdw/hba_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"

PG_MODULE_MAGIC;

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct hbaFdwOption
{
	const char *optname;
	Oid         optcontext;     /* Oid of catalog in which option may appear */
};

/*
 * FDW-specific information for RelOptInfo.fdw_private.
 */
typedef struct hbaFdwPlanState
{
	char	*hbafilename; /* filename from the GUC hba_file */
} hbaFdwPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct hbaFdwExecutionState
{
	char *hbafilename;
} hbaFdwExecutionState;

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(hba_fdw_handler);
PG_FUNCTION_INFO_V1(hba_fdw_validator);

/*
 * FDW callbacks routines
 */
#define HBA_FDW_GETFOREIGNRELSIZE_PROTO PlannerInfo *root, \
										RelOptInfo *baserel, \
										Oid foreigntableid
#define HBA_FDW_GETFOREIGNPATHS_PROTO PlannerInfo *root, \
									  RelOptInfo *baserel, \
									  Oid foreigntableid
#define HBA_FDW_GETFOREIGNPLAN_PROTO PlannerInfo *root, \
									 RelOptInfo *baserel, \
									 Oid foreigntableid, \
									 ForeignPath *best_path, \
									 List *tlist, \
									 List *scan_clauses, \
									 Plan *outer_plan
#define HBA_FDW_EXPLAINFOREIGNSCAN_PROTO ForeignScanState *node, ExplainState *es
#define HBA_FDW_BEGINFOREIGNSCAN_PROTO ForeignScanState *node, int eflags
#define HBA_FDW_ITERATEFOREIGNSCAN_PROTO ForeignScanState *node
#define HBA_FDW_RESCANFOREIGNSCAN_PROTO ForeignScanState *node
#define HBA_FDW_ENDFOREIGNSCAN_PROTO ForeignScanState *node
#define HBA_FDW_ANALYZEFOREIGNTABLE_PROTO Relation relation, \
										  AcquireSampleRowsFunc *func, \
										  BlockNumber *totalpages
#define HBA_FDW_ISFOREIGNSCANPARALLELSAFE_PROTO PlannerInfo *root, RelOptInfo *rel, \
												RangeTblEntry *rte

static void hbaGetForeignRelSize(HBA_FDW_GETFOREIGNRELSIZE_PROTO);
static void hbaGetForeignPaths(HBA_FDW_GETFOREIGNPATHS_PROTO);
static ForeignScan *hbaGetForeignPlan(HBA_FDW_GETFOREIGNPLAN_PROTO);
static void hbaExplainForeignScan(HBA_FDW_EXPLAINFOREIGNSCAN_PROTO);
static void hbaBeginForeignScan(HBA_FDW_BEGINFOREIGNSCAN_PROTO);
static TupleTableSlot *hbaIterateForeignScan(HBA_FDW_ITERATEFOREIGNSCAN_PROTO);
static void hbaReScanForeignScan(HBA_FDW_RESCANFOREIGNSCAN_PROTO);
static void hbaEndForeignScan(HBA_FDW_ENDFOREIGNSCAN_PROTO);
static bool hbaAnalyzeForeignTable(HBA_FDW_ANALYZEFOREIGNTABLE_PROTO);
static bool hbaIsForeignScanParallelSafe(HBA_FDW_ISFOREIGNSCANPARALLELSAFE_PROTO);


/*
 * Helper functions
 */

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
hba_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	elog(DEBUG1, "entering function %s", PG_FUNCNAME_MACRO);

	fdwroutine->GetForeignRelSize = hbaGetForeignRelSize;
	fdwroutine->GetForeignPaths = hbaGetForeignPaths;
	fdwroutine->GetForeignPlan = hbaGetForeignPlan;
	fdwroutine->ExplainForeignScan = hbaExplainForeignScan;
	fdwroutine->BeginForeignScan = hbaBeginForeignScan;
	fdwroutine->IterateForeignScan = hbaIterateForeignScan;
	fdwroutine->ReScanForeignScan = hbaReScanForeignScan;
	fdwroutine->EndForeignScan = hbaEndForeignScan;
	fdwroutine->AnalyzeForeignTable = hbaAnalyzeForeignTable;
	fdwroutine->IsForeignScanParallelSafe = hbaIsForeignScanParallelSafe;

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
hba_fdw_validator(PG_FUNCTION_ARGS)
{
	elog(DEBUG1, "entering function %s", PG_FUNCNAME_MACRO);

	PG_RETURN_VOID();
}

/*
 * hbaGetForeignRelSize
 *      Obtain relation size estimates for a foreign table
 */
static void
hbaGetForeignRelSize(HBA_FDW_GETFOREIGNRELSIZE_PROTO)
{
	elog(DEBUG1, "entering function %s", PG_FUNCNAME_MACRO);
}

/*
 * hbaGetForeignPaths
 *      Create possible access paths for a scan on the foreign table
 *
 *      Currently we don't support any push-down feature, so there is only one
 *      possible access path, which simply returns all records in the order in
 *      the data file.
 */
static void
hbaGetForeignPaths(HBA_FDW_GETFOREIGNPATHS_PROTO)
{
    Cost        startup_cost;
    Cost        total_cost;

	elog(DEBUG1, "entering function %s", PG_FUNCNAME_MACRO);

	startup_cost = 0;
	total_cost = startup_cost + baserel->rows;

	/* Create a ForeignPath node and add it as only possible path */
    add_path(baserel, (Path *)
             create_foreignscan_path(root, baserel,
                                     NULL,  /* default pathtarget */
                                     baserel->rows,
                                     startup_cost,
                                     total_cost,
                                     NIL,   /* no pathkeys */
                                     baserel->lateral_relids,
                                     NULL,  /* no extra plan */
                                     NIL)); /* no fdw private data */
}

/*
 * hbaGetForeignPlan
 *      Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
hbaGetForeignPlan(HBA_FDW_GETFOREIGNPLAN_PROTO)
{
    Index       scan_relid = baserel->relid;

	elog(DEBUG1, "entering function %s", PG_FUNCNAME_MACRO);

    /*
     * We have no native ability to evaluate restriction clauses, so we just
     * put all the scan_clauses into the plan node's qual list for the
     * executor to check.  So all we have to do here is strip RestrictInfo
     * nodes from the clauses and ignore pseudoconstants (which will be
     * handled elsewhere).
     */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Create the ForeignScan node */
    return make_foreignscan(tlist,
                            scan_clauses,
                            scan_relid,
                            NIL,    /* no expressions to evaluate */
                            best_path->fdw_private,
                            NIL,    /* no custom tlist */
                            NIL,    /* no remote quals */
                            outer_plan);
}

/*
 * hbaExplainForeignScan
 *      Produce extra output for EXPLAIN
 */
static void
hbaExplainForeignScan(HBA_FDW_EXPLAINFOREIGNSCAN_PROTO)
{
	elog(DEBUG1, "entering function %s", PG_FUNCNAME_MACRO);

	ExplainPropertyText("Foreign Hba File", HbaFileName, es);
}

/*
 * hbaBeginForeignScan
 *      Initiate access to the hba file by creating CopyState
 */
static void
hbaBeginForeignScan(HBA_FDW_BEGINFOREIGNSCAN_PROTO)
{
	elog(DEBUG1, "entering function %s", PG_FUNCNAME_MACRO);

    /*
     * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;
}

/*
 * hbaIterateForeignScan
 *      Read next record from the data file and store it into the
 *      ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
hbaIterateForeignScan(HBA_FDW_ITERATEFOREIGNSCAN_PROTO)
{
	elog(DEBUG1, "entering function %s", PG_FUNCNAME_MACRO);
	return node->ss.ss_ScanTupleSlot;
}

/*
 * hbaReScanForeignScan
 *      Rescan table, possibly with new parameters
 */
static void
hbaReScanForeignScan(HBA_FDW_RESCANFOREIGNSCAN_PROTO)
{
	elog(DEBUG1, "entering function %s", PG_FUNCNAME_MACRO);
}

/*
 * hbaEndForeignScan
 *      Finish scanning foreign table and dispose objects used for this scan
 */
static void
hbaEndForeignScan(HBA_FDW_ENDFOREIGNSCAN_PROTO)
{
	elog(DEBUG1, "entering function %s", PG_FUNCNAME_MACRO);
}

/*
 * hbaAnalyzeForeignTable
 *      Test whether analyzing this foreign table is supported
 */
static bool
hbaAnalyzeForeignTable(HBA_FDW_ANALYZEFOREIGNTABLE_PROTO)
{
	elog(DEBUG1, "entering function %s", PG_FUNCNAME_MACRO);

	return false;
}

/*
 * hbaIsForeignScanParallelSafe
 *      Reading the pg_hba.conf file in a parallel worker should work
 *      just the same as reading it in the leader, so mark scans safe.
 */
static bool 
hbaIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
                              RangeTblEntry *rte)
{
    return true;
}
