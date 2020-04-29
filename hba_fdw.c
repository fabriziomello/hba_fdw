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

#include <sys/stat.h>

#include "commands/copy.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/sampling.h"

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
	BlockNumber pages;          /* estimate of file's physical size */
	double      ntuples;        /* estimate of number of data rows */
} hbaFdwPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct hbaFdwExecutionState
{
	/* nothing */
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
static void estimate_size(PlannerInfo *root, RelOptInfo *baserel,
						  hbaFdwPlanState *fdw_private);
static void estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
						   hbaFdwPlanState *fdw_private,
						   Cost *startup_cost, Cost *total_cost);
static int	file_acquire_sample_rows(Relation onerel, int elevel,
									 HeapTuple *rows, int targrows,
									 double *totalrows, double *totaldeadrows);

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
	hbaFdwPlanState *fdw_private;

	/* Set the name of file based on global HbaFileName */
	fdw_private = (hbaFdwPlanState *) palloc(sizeof(hbaFdwPlanState));
	baserel->fdw_private = (void *) fdw_private;

	/* Estimate relation size */
	estimate_size(root, baserel, fdw_private);
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
	hbaFdwPlanState *fdw_private = (hbaFdwPlanState *) baserel->fdw_private;
    Cost        startup_cost;
    Cost        total_cost;

	elog(DEBUG1, "entering function %s", PG_FUNCNAME_MACRO);

	/* Estimate costs */
	estimate_costs(root, baserel, fdw_private,
				   &startup_cost, &total_cost);

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
	struct stat stat_buf;

	/*
	 * Get size of the file.  (XXX if we fail here, would it be better to just
	 * return false to skip analyzing the table?)
	 */
	if (stat(HbaFileName, &stat_buf) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m",
						HbaFileName)));

    /*
     * Convert size to pages.  Must return at least 1 so that we can tell
     * later on that pg_class.relpages is not default.
     */
    *totalpages = (stat_buf.st_size + (BLCKSZ - 1)) / BLCKSZ;
    if (*totalpages < 1)
		*totalpages = 1;

	*func = file_acquire_sample_rows;

	return true;
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

/*
 * Estimate size of a foreign table.
 *
 * The main result is returned in baserel->rows.  We also set
 * fdw_private->pages and fdw_private->ntuples for later use in the cost
 * calculation.
 */
static void
estimate_size(PlannerInfo *root, RelOptInfo *baserel,
              hbaFdwPlanState *fdw_private)
{
    struct stat stat_buf;
    BlockNumber pages;
    double      ntuples;
    double      nrows;

    /*
     * Get size of the file. It might not be there at plan time, though, in
     * which case we have to use a default estimate.
     */
    if (stat(HbaFileName, &stat_buf) < 0)
		stat_buf.st_size = 10 * BLCKSZ;

    /*
     * Convert size to pages for use in I/O cost estimate later.
     */
    pages = (stat_buf.st_size + (BLCKSZ - 1)) / BLCKSZ;
    if (pages < 1)
        pages = 1;
    fdw_private->pages = pages;

    /*
     * Estimate the number of tuples in the file.
     */
    if (baserel->pages > 0)
    {
        /*
         * We have # of pages and # of tuples from pg_class (that is, from a
         * previous ANALYZE), so compute a tuples-per-page estimate and scale
         * that by the current file size.
         */
        double      density;

        density = baserel->tuples / (double) baserel->pages;
        ntuples = clamp_row_est(density * (double) pages);
    }
    else
    {
        /*
         * Otherwise we have to fake it.  We back into this estimate using the
         * planner's idea of the relation width; which is bogus if not all
         * columns are being read, not to mention that the text representation
         * of a row probably isn't the same size as its internal
         * representation.  Possibly we could do something better, but the
         * real answer to anyone who complains is "ANALYZE" ...
         */
        int         tuple_width;

        tuple_width = MAXALIGN(baserel->reltarget->width) +
            MAXALIGN(SizeofHeapTupleHeader);
        ntuples = clamp_row_est((double) stat_buf.st_size /
                                (double) tuple_width);
    }
    fdw_private->ntuples = ntuples;

    /*
     * Now estimate the number of rows returned by the scan after applying the
     * baserestrictinfo quals.
     */
    nrows = ntuples *
        clauselist_selectivity(root,
                               baserel->baserestrictinfo,
                               0,
                               JOIN_INNER,
                               NULL);

    nrows = clamp_row_est(nrows);

    /* Save the output-rows estimate for the planner */
    baserel->rows = nrows;
}

/*
 * Estimate costs of scanning a foreign table.
 *
 * Results are returned in *startup_cost and *total_cost.
 */
static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   hbaFdwPlanState *fdw_private,
			   Cost *startup_cost, Cost *total_cost)
{
	BlockNumber pages = fdw_private->pages;
	double		ntuples = fdw_private->ntuples;
	Cost		run_cost = 0;
	Cost		cpu_per_tuple;

	/*
	 * We estimate costs almost the same way as cost_seqscan(), thus assuming
	 * that I/O costs are equivalent to a regular table file of the same size.
	 * However, we take per-tuple CPU costs as 10x of a seqscan, to account
	 * for the cost of parsing records.
	 *
	 * In the case of a program source, this calculation is even more divorced
	 * from reality, but we have no good alternative; and it's not clear that
	 * the numbers we produce here matter much anyway, since there's only one
	 * access path for the rel.
	 */
	run_cost += seq_page_cost * pages;

	*startup_cost = baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost * 10 + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * ntuples;
	*total_cost = *startup_cost + run_cost;
}

/*
 * file_acquire_sample_rows -- acquire a random sample of rows from the table
 *
 * Selected rows are returned in the caller-allocated array rows[],
 * which must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also count the total number of rows in the file and return it into
 * *totalrows.  Note that *totaldeadrows is always set to 0.
 *
 * Note that the returned list of rows is not always in order by physical
 * position in the file.  Therefore, correlation estimates derived later
 * may be meaningless, but it's OK because we don't use the estimates
 * currently (the planner only pays attention to correlation for indexscans).
 */
static int
file_acquire_sample_rows(Relation onerel, int elevel,
						 HeapTuple *rows, int targrows,
						 double *totalrows, double *totaldeadrows)
{
	int			numrows = 0;
	double		rowstoskip = -1;	/* -1 means not set yet */
	ReservoirStateData rstate;
	TupleDesc	tupDesc;
	Datum	   *values;
	bool	   *nulls;
	bool		found;
	CopyState	cstate;
	ErrorContextCallback errcallback;
	MemoryContext oldcontext = CurrentMemoryContext;
	MemoryContext tupcontext;

	Assert(onerel);
	Assert(targrows > 0);

	tupDesc = RelationGetDescr(onerel);
	values = (Datum *) palloc(tupDesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(tupDesc->natts * sizeof(bool));

	/*
	 * Create CopyState from FDW options.
	 */
	cstate = BeginCopyFrom(NULL, onerel, HbaFileName, false, NULL, NIL, NIL);

	/*
	 * Use per-tuple memory context to prevent leak of memory used to read
	 * rows from the file with Copy routines.
	 */
	tupcontext = AllocSetContextCreate(CurrentMemoryContext,
									   "file_fdw temporary context",
									   ALLOCSET_DEFAULT_SIZES);

	/* Prepare for sampling rows */
	reservoir_init_selection_state(&rstate, targrows);

	/* Set up callback to identify error line number. */
	errcallback.callback = CopyFromErrorCallback;
	errcallback.arg = (void *) cstate;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	*totalrows = 0;
	*totaldeadrows = 0;
	for (;;)
	{
		/* Check for user-requested abort or sleep */
		vacuum_delay_point();

		/* Fetch next row */
		MemoryContextReset(tupcontext);
		MemoryContextSwitchTo(tupcontext);

		found = NextCopyFrom(cstate, NULL, values, nulls);

		MemoryContextSwitchTo(oldcontext);

		if (!found)
			break;

		/*
		 * The first targrows sample rows are simply copied into the
		 * reservoir.  Then we start replacing tuples in the sample until we
		 * reach the end of the relation. This algorithm is from Jeff Vitter's
		 * paper (see more info in commands/analyze.c).
		 */
		if (numrows < targrows)
		{
			rows[numrows++] = heap_form_tuple(tupDesc, values, nulls);
		}
		else
		{
			/*
			 * t in Vitter's paper is the number of records already processed.
			 * If we need to compute a new S value, we must use the
			 * not-yet-incremented value of totalrows as t.
			 */
			if (rowstoskip < 0)
				rowstoskip = reservoir_get_next_S(&rstate, *totalrows, targrows);

			if (rowstoskip <= 0)
			{
				/*
				 * Found a suitable tuple, so save it, replacing one old tuple
				 * at random
				 */
				int			k = (int) (targrows * sampler_random_fract(rstate.randstate));

				Assert(k >= 0 && k < targrows);
				heap_freetuple(rows[k]);
				rows[k] = heap_form_tuple(tupDesc, values, nulls);
			}

			rowstoskip -= 1;
		}

		*totalrows += 1;
	}

	/* Remove error callback. */
	error_context_stack = errcallback.previous;

	/* Clean up. */
	MemoryContextDelete(tupcontext);

	EndCopyFrom(cstate);

	pfree(values);
	pfree(nulls);

	/*
	 * Emit some interesting relation info
	 */
	ereport(elevel,
			(errmsg("\"%s\": file contains %.0f rows; "
					"%d rows in sample",
					RelationGetRelationName(onerel),
					*totalrows, numrows)));

	return numrows;
}
