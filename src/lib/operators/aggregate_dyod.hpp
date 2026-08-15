#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_aggregate_operator.hpp"
#include "expression/window_function_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * Parallel hash-based aggregation operator. It supports MIN, MAX, SUM, AVG, COUNT (incl. COUNT(*) and
 * COUNT(DISTINCT)), STDDEV_SAMP, and ANY, and produces a table of (chunked) value segments with the schema
 * [group-by columns..., aggregate columns...]. Like the other aggregate operators, it makes no guarantees about the
 * order of the output rows.
 *
 * Two execution paths exist. Without group-by columns (`no_groupby_aggregate`), each worker aggregates the chunks it
 * pulls from a shared counter into its own set of aggregate states. Then the per-worker states are merged and
 * finalized into the single output row using the `OperatorSharedState` helper.
 *
 * The group-by path (`groupby_aggregate`) runs in the following phases (see aggregate_dyod_utils/ticketing.hpp for
 * phase 1, aggregate_dyod.cpp for the rest):
 *
 *  1. Ticketing (`_compute_groups`): Assigns each distinct combination of group-by values a dense id or offset into 
 *     the result, its "ticket".
 *     The result is one ticket per input row plus the group count. All later phases address per-group data by ticket
 *     instead of re-hashing group-by values. Ticketing itself runs in three steps:
 *
 *      a. Cardinality estimation (aggregate_dyod_utils/cardinality_estimation.hpp): A parallel pass feeds the
 *         hashes of all group-by keys into per-worker HyperLogLog sketches, which are merged into one estimate
 *         (using 1024 registers giving ~3.25% standard error). Its three-sigma upper bound sizes the ticket hash table
 *         below, which is designed to have a fixed capacity (growing is supported but a slow fallback for when the 
 *         estimate is off).
 *
 *.     PER-CHUNK PARALLEL:
 *      b. Materialization (`_materialize_rows`, not necessary for non-string single-column GROUPBY's): Each chunk's 
 *.        group-by values are packed into fixed-format key rows (aggregate_dyod_utils/ticketing.hpp):
 *.        An optional NULL bitmap followed by the inline values, so that hashing and equality are a simple pass over
 *         contiguous bytes. 
 *         Strings store [length, prefix] inline. Only strings longer than the prefix are compared through a pointer,
 *         which points directly into the source segment where possible and into a copy arena otherwise.
 *
 *      c. Ticket assignment (`ConcurrentTicketMap`, aggregate_dyod_utils/concurrent_ticket_map.hpp): All threads
 *         probe one shared, lock-free, linear-probing hash table whose slots are claimed and published via a single
 *         atomic state word. `try_emplace` accepts the thread's next candidate ticket and returns the group's actual
 *         ticket if it already exists. If not it returns the newly assigned ticket. To keep threads from fighting over
 *         a global ticket counter "fuzzy ticketing" is used. Each thread hands out tickets from its own pre-claimed
 *         range of size 1024. The unused trailing tickets of each range are compacted out afterwards so the final
 *         tickets form a [0, group_count) range. 
 *
 *     A single non-string group-by column takes a fast path that skips step (b). The value itself is the key of the
 *     `ConcurrentTicketMap`, no rows are materialized, and no key-row hash table is kept for phase 3.
 *
 *  2. Accumulation (`_delegate_accumulate`): We spawn a job per worker/thread. This job selects an aggregate and pulls
 *     chunks from a per-aggregate counter. Each job accumulates rows into a bounded thread-local hash table keyed by
 *     ticket, which is spilled into the shared per-group intermediate states whenever it grows too large (the merge
 *     of a group's state is guarded by one atomic flag per group). A regular spill probes each group's flag exactly
 *     once and never waits. Entries whose flag is currently held by another thread are simply kept in the local
 *     table for a later attempt. Only a forced spill (used when the local table must be emptied, e.g. at the end of
 *     a job) spins on contended flags. When an aggregate's chunks are exhausted, the job continues with the next one.
 *
 *  3. Building the actual materialized result table happens in three steps:
 *     1. Group-by column output (`_build_groupby_output_columns`): 
 *          Each group's group-by column values are recovered. For low-cardinality group-bys by reading the key rows
 *          straight from the ticketing hash table, otherwise by re-scanning the source columns, where the first row
 *          of each group claims the value.
 *
 *     2. Finalization (`_finalize_grouped_aggregates`): 
 *          Batched jobs turn the intermediate states into the final per-group values and NULL information (e.g., 
 *          dividing sum by count for AVG).
 *
 *     3. Emission (`_emit_aggregate_columns`): 
 *          All phases write their results directly as chunk-sized pieces (using `ChunkedVector`), so assembling the 
 *          output table is only moving the pieces into value segments of the output chunks without a copy.
 *
 * PROs and CONs of this design:
 *   PRO:
 *       - In hot-loops the only synchronizations are the atomics in the ticketing hash table and the per-group ones
 *         for the intermediate states. 
 *       - The ticket is only fought over once per group, and the intermediate state is only fought over when the 
 *         local hash table spills. For low-cardinality we have very few groups so the ticketing is cheap and the 
 *         local-hash table reduces the amount of contention. For high cardinality we almost never have collisions for
 *         tickets and states.
 *       - As a result the algorithm scales very well especially for large inputs and high thread counts.
 *   CON: 
 *       - We pass the data three times: Once for HyperLogLog (cardinality estimation), once for ticketing, and once
 *         for aggregation (+ finalization). When few threads are used the overhead for parallelization is unjustified.
 *       - For high cardinalities we have to allocate a large ticketing hash table, a large intermediate result and the
 *         result.
 *       - The hash table for ticketing has an inherently random access pattern, so it is not cache-friendly.
 */
class AggregateDYOD : public AbstractAggregateOperator {
 public:
  AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  std::shared_ptr<const Table> no_groupby_aggregate();
  std::shared_ptr<const Table> groupby_aggregate();

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;
};

}  // namespace hyrise
