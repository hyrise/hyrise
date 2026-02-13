#include <algorithm>
#include <chrono>
#include <functional>
#include <iomanip>
#include <immintrin.h> // AVX2
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <tbb/parallel_sort.h>  // Intel TBB for better parallel sorting

#include "hyrise.hpp"
#include "types.hpp"
#include "storage/table.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "benchmark_config.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/sort_for_aggregate.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "scheduler/job_task.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/value_segment.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

/**
 * CONFIGURATION
 * 
 * Edit this struct to change the run configuration without modifying the whole main().
 * TODO: Make this command line arguments for SLURM script execution
 */
struct PlaygroundConfig {
  // scale_factor removed - now a loop variable in main()
  uint32_t num_workers = 32;    // Number of workers for Multi-Threaded variants
  uint32_t num_iterations = 7;  // Number of benchmark iterations per algorithm
  bool run_single_baseline = true;
  bool run_single_optimized = true;
  bool run_multi_naive = true;
  bool run_multi_optimized = true;
};

// Global Config Instance
auto CONFIG = PlaygroundConfig{};



std::shared_ptr<Table> generate_lineitem_data(float scale_factor) {
  std::cout << "- Generating TPC-H LineItem (SF " << scale_factor << ")..." << std::endl;
  auto generator = TPCHTableGenerator{scale_factor, ClusteringConfiguration::None};
  auto tables = generator.generate();
  return tables["lineitem"].table;
}

/**
 * SCHEDULER SETUP
 * Note: set_scheduler() calls finish() on old scheduler and begin() on new scheduler automatically.
 */
void setup_scheduler(bool multi_threaded, uint32_t worker_count = 1) {
  if (multi_threaded) {
    Hyrise::get().topology.use_non_numa_topology(worker_count);
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  } else {
    Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
  }
}

/**
 * HASH AGGREGATION IMPLEMENTATIONS
 */

// Key encoding for composite keys (l_returnflag, l_linestatus) - both are single chars.
// Encodes as uint16_t for efficient hashing and atomic operations (in multi-threaded version).
using EncodedKey = uint16_t;

inline EncodedKey encode_key(char rf, char ls) {
  return static_cast<EncodedKey>(static_cast<uint8_t>(rf) |
         (static_cast<uint16_t>(static_cast<uint8_t>(ls)) << 8));
}

inline std::pair<char, char> decode_key(EncodedKey encoded) {
  return {static_cast<char>(encoded & 0xFF),
          static_cast<char>((encoded >> 8) & 0xFF)};
}

std::shared_ptr<Table> hash_single_baseline(const std::shared_ptr<Table>& input) {
  // 1. Prepare Input
  auto table_wrapper = std::make_shared<TableWrapper>(input);
  table_wrapper->execute();

  // 2. Define Columns
  // Group By: l_returnflag, l_linestatus
  // Aggregate: SUM(l_quantity)
  const auto returnflag_col_id = input->column_id_by_name("l_returnflag");
  const auto linestatus_col_id = input->column_id_by_name("l_linestatus");
  const auto quantity_col_id = input->column_id_by_name("l_quantity");

  auto quantity_expr = std::make_shared<PQPColumnExpression>(quantity_col_id, input->column_data_type(quantity_col_id), input->column_is_nullable(quantity_col_id), input->column_name(quantity_col_id));

  // 3. Define Aggregate Expressions
  // usage: sum_(expression)
  auto sum_expr = expression_functional::sum_(quantity_expr);

  // 4. Create and Run Operator
  auto aggregate_op = std::make_shared<AggregateHash>(
      table_wrapper,
      std::vector<std::shared_ptr<WindowFunctionExpression>>{sum_expr},
      std::vector<ColumnID>{returnflag_col_id, linestatus_col_id}
  );

  aggregate_op->execute();

  return std::const_pointer_cast<Table>(aggregate_op->get_output());
}

std::shared_ptr<Table> hash_single_optimized(const std::shared_ptr<Table>& input) {
  // Single-threaded version of the ticketing approach from hash_multi_optimized.
  // Uses the same key encoding but simplified (no atomics, no FuzzyTicketer).

  const auto chunk_count = input->chunk_count();

  // Column IDs
  const auto returnflag_col_id = input->column_id_by_name("l_returnflag");
  const auto linestatus_col_id = input->column_id_by_name("l_linestatus");
  const auto quantity_col_id = input->column_id_by_name("l_quantity");

  // Simple hash table: EncodedKey -> ticket
  // Pre-sized for ~50% load factor with 6 groups
  auto key_to_ticket = std::unordered_map<EncodedKey, uint32_t>{};
  key_to_ticket.reserve(16);

  // Aggregates indexed by ticket
  auto partial_sums = std::vector<double>{};
  partial_sums.reserve(8);

  // Simple ticket counter
  uint32_t next_ticket = 0;

  // Process all chunks
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = input->get_chunk(chunk_id);
    if (!chunk) continue;

    const auto chunk_size = chunk->size();
    const auto& rf_seg = *chunk->get_segment(returnflag_col_id);
    const auto& ls_seg = *chunk->get_segment(linestatus_col_id);
    const auto& qty_seg = *chunk->get_segment(quantity_col_id);

    // Materialize values for this chunk
    auto rf_vals = std::vector<char>{};
    auto ls_vals = std::vector<char>{};
    auto qty_vals = std::vector<float>{};
    rf_vals.reserve(chunk_size);
    ls_vals.reserve(chunk_size);
    qty_vals.reserve(chunk_size);

    segment_iterate<pmr_string>(rf_seg, [&](const auto& pos) {
      rf_vals.push_back(pos.is_null() ? '\0' : pos.value()[0]);
    });
    segment_iterate<pmr_string>(ls_seg, [&](const auto& pos) {
      ls_vals.push_back(pos.is_null() ? '\0' : pos.value()[0]);
    });
    segment_iterate<float>(qty_seg, [&](const auto& pos) {
      qty_vals.push_back(pos.is_null() ? 0.0f : pos.value());
    });

    // Process each row: get or assign ticket, update aggregate
    for (size_t i = 0; i < rf_vals.size(); ++i) {
      EncodedKey key = encode_key(rf_vals[i], ls_vals[i]);

      // Get or assign ticket
      auto [it, inserted] = key_to_ticket.try_emplace(key, next_ticket);
      if (inserted) {
        partial_sums.push_back(0.0);
        ++next_ticket;
      }
      uint32_t ticket = it->second;

      // Update aggregate
      partial_sums[ticket] += static_cast<double>(qty_vals[i]);
    }
  }

  // Build output table
  auto column_definitions = TableColumnDefinitions{
      TableColumnDefinition{"l_returnflag", DataType::String, false},
      TableColumnDefinition{"l_linestatus", DataType::String, false},
      TableColumnDefinition{"sum_quantity", DataType::Double, false}};

  auto output = std::make_shared<Table>(column_definitions, TableType::Data);

  auto rf_out = pmr_vector<pmr_string>{};
  auto ls_out = pmr_vector<pmr_string>{};
  auto sum_out = pmr_vector<double>{};

  rf_out.reserve(next_ticket);
  ls_out.reserve(next_ticket);
  sum_out.reserve(next_ticket);

  for (const auto& [encoded_key, ticket] : key_to_ticket) {
    auto [rf, ls] = decode_key(encoded_key);
    rf_out.push_back(pmr_string(1, rf));
    ls_out.push_back(pmr_string(1, ls));
    sum_out.push_back(partial_sums[ticket]);
  }

  auto segments = Segments{};
  segments.push_back(std::make_shared<ValueSegment<pmr_string>>(std::move(rf_out)));
  segments.push_back(std::make_shared<ValueSegment<pmr_string>>(std::move(ls_out)));
  segments.push_back(std::make_shared<ValueSegment<double>>(std::move(sum_out)));

  output->append_chunk(segments);

  return output;
}

std::shared_ptr<Table> hash_multi_naive(const std::shared_ptr<Table>& input) {
  const auto num_workers = CONFIG.num_workers;
  const auto chunk_count = input->chunk_count();

  // Column IDs
  const auto returnflag_col_id = input->column_id_by_name("l_returnflag");
  const auto linestatus_col_id = input->column_id_by_name("l_linestatus");
  const auto quantity_col_id = input->column_id_by_name("l_quantity");

  // Custom hash for pair<pmr_string, pmr_string>
  struct PairHash {
    std::size_t operator()(const std::pair<pmr_string, pmr_string>& p) const {
      auto h1 = std::hash<std::string_view>{}(p.first);
      auto h2 = std::hash<std::string_view>{}(p.second);
      return h1 ^ (h2 << 1);
    }
  };
  using AggregateKey = std::pair<pmr_string, pmr_string>;
  using PartialMap = std::unordered_map<AggregateKey, double, PairHash>;

  // Pre-allocate partial maps (one per worker, no synchronization needed)
  auto partial_maps = std::vector<PartialMap>(num_workers);

  // PHASE 1: Parallel aggregation with JobTasks
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(num_workers);

  for (uint32_t worker_id = 0; worker_id < num_workers; ++worker_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, worker_id] {
      auto& local_map = partial_maps[worker_id];

      // Round-robin chunk assignment
      for (auto chunk_id = ChunkID{worker_id};
           static_cast<ChunkID::base_type>(chunk_id) < static_cast<ChunkID::base_type>(chunk_count);
           chunk_id = ChunkID{static_cast<ChunkID::base_type>(chunk_id) + num_workers}) {

        const auto chunk = input->get_chunk(chunk_id);
        if (!chunk) continue;

        const auto chunk_size = chunk->size();
        const auto& rf_seg = *chunk->get_segment(returnflag_col_id);
        const auto& ls_seg = *chunk->get_segment(linestatus_col_id);
        const auto& qty_seg = *chunk->get_segment(quantity_col_id);

        // Materialize values for this chunk
        auto rf_vals = std::vector<pmr_string>{};
        auto ls_vals = std::vector<pmr_string>{};
        auto qty_vals = std::vector<float>{};
        rf_vals.reserve(chunk_size);
        ls_vals.reserve(chunk_size);
        qty_vals.reserve(chunk_size);

        segment_iterate<pmr_string>(rf_seg, [&](const auto& pos) {
          rf_vals.push_back(pos.is_null() ? pmr_string{} : pmr_string{pos.value()});
        });
        segment_iterate<pmr_string>(ls_seg, [&](const auto& pos) {
          ls_vals.push_back(pos.is_null() ? pmr_string{} : pmr_string{pos.value()});
        });
        segment_iterate<float>(qty_seg, [&](const auto& pos) {
          qty_vals.push_back(pos.is_null() ? 0.0f : pos.value());
        });

        // Aggregate into local map
        for (size_t i = 0; i < rf_vals.size(); ++i) {
          auto key = std::make_pair(rf_vals[i], ls_vals[i]);
          local_map[key] += static_cast<double>(qty_vals[i]);
        }
      }
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  // PHASE 2: Merge partial results (single-threaded)
  PartialMap merged_map;
  for (const auto& partial_map : partial_maps) {
    for (const auto& [key, sum] : partial_map) {
      merged_map[key] += sum;
    }
  }

  // PHASE 3: Build output table
  auto column_definitions = TableColumnDefinitions{
    TableColumnDefinition{"l_returnflag", DataType::String, false},
    TableColumnDefinition{"l_linestatus", DataType::String, false},
    TableColumnDefinition{"sum_quantity", DataType::Double, false}
  };

  auto output = std::make_shared<Table>(column_definitions, TableType::Data);

  auto rf_out = pmr_vector<pmr_string>{};
  auto ls_out = pmr_vector<pmr_string>{};
  auto sum_out = pmr_vector<double>{};

  rf_out.reserve(merged_map.size());
  ls_out.reserve(merged_map.size());
  sum_out.reserve(merged_map.size());

  for (const auto& [key, sum] : merged_map) {
    rf_out.push_back(key.first);
    ls_out.push_back(key.second);
    sum_out.push_back(sum);
  }

  auto segments = Segments{};
  segments.push_back(std::make_shared<ValueSegment<pmr_string>>(std::move(rf_out)));
  segments.push_back(std::make_shared<ValueSegment<pmr_string>>(std::move(ls_out)));
  segments.push_back(std::make_shared<ValueSegment<double>>(std::move(sum_out)));

  output->append_chunk(segments);

  return output;
}

/**
 * DATA STRUCTURES FOR MULTI-THREADED OPTIMIZED HASH AGGREGATION
 * Based on "Global Hash Tables Strike Back" paper.
 *
 * Key insight: Use ticketing to separate hash table operations from aggregate updates.
 * Traditional:  Key -> [Hash Table] -> Aggregate (contention on updates)
 * Paper's Way:  Key -> [Hash Table] -> Ticket -> [Vector] -> Aggregate
 *
 * Note: EncodedKey, encode_key, decode_key are defined above (used by both single and multi).
 */

// Fuzzy Ticketer: reduces contention by batching ticket allocations per thread
class FuzzyTicketer {
 public:
  static constexpr uint32_t BATCH_SIZE = 64;

  FuzzyTicketer() : _global_counter(0) {}

  struct LocalState {
    uint32_t next_ticket = 0;
    uint32_t batch_end = 0;

    bool has_tickets() const { return next_ticket < batch_end; }
  };

  uint32_t get_ticket(LocalState& local) {
    if (!local.has_tickets()) {
      uint32_t batch_start = _global_counter.fetch_add(BATCH_SIZE, std::memory_order_relaxed);
      local.next_ticket = batch_start;
      local.batch_end = batch_start + BATCH_SIZE;
    }
    return local.next_ticket++;
  }

  uint32_t max_ticket() const {
    return _global_counter.load(std::memory_order_acquire);
  }

 private:
  std::atomic<uint32_t> _global_counter;
};

// Folklore Hash Table: lock-free linear probing with ticketing
// Slot states: 0 = empty, 1 = write in progress, >= 2 = ticket + 2
class FolkloreHashTable {
 public:
  static constexpr uint32_t EMPTY = 0;
  static constexpr uint32_t WRITE_IN_PROGRESS = 1;
  static constexpr uint32_t TICKET_OFFSET = 2;

  struct Slot {
    std::atomic<uint32_t> state{EMPTY};
    std::atomic<EncodedKey> key{0};
  };

  explicit FolkloreHashTable(size_t capacity)
      : _capacity(capacity), _mask(capacity - 1), _slots(capacity) {
    // Capacity must be power of 2 for fast modulo
    Assert((capacity & (capacity - 1)) == 0, "Capacity must be power of 2");
  }

  // GET_OR_INSERT: returns existing ticket or inserts new one
  uint32_t get_or_insert(EncodedKey key, FuzzyTicketer& ticketer,
                         FuzzyTicketer::LocalState& local_ticketer) {
    const size_t hash = std::hash<EncodedKey>{}(key);
    size_t idx = hash & _mask;

    while (true) {
      uint32_t state = _slots[idx].state.load(std::memory_order_acquire);

      if (state >= TICKET_OFFSET) {
        // Slot occupied with a ticket - check if key matches
        EncodedKey existing_key = _slots[idx].key.load(std::memory_order_acquire);
        if (existing_key == key) {
          return state - TICKET_OFFSET;  // Found - return ticket
        }
        // Collision - linear probe
        idx = (idx + 1) & _mask;
        continue;
      }

      if (state == EMPTY) {
        // Try to claim this slot
        uint32_t expected = EMPTY;
        if (_slots[idx].state.compare_exchange_strong(
                expected, WRITE_IN_PROGRESS,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
          // Successfully claimed - assign ticket and store key
          uint32_t ticket = ticketer.get_ticket(local_ticketer);
          _slots[idx].key.store(key, std::memory_order_release);
          _slots[idx].state.store(ticket + TICKET_OFFSET, std::memory_order_release);
          return ticket;
        }
        // CAS failed - retry from this slot
        continue;
      }

      if (state == WRITE_IN_PROGRESS) {
        // Another thread is writing - spin briefly
        #if defined(__x86_64__) || defined(_M_X64)
        __builtin_ia32_pause();
        #endif
        continue;
      }
    }
  }

  // Iterate all entries for final materialization
  template <typename Func>
  void for_each_entry(Func&& func) const {
    for (size_t i = 0; i < _capacity; ++i) {
      uint32_t state = _slots[i].state.load(std::memory_order_acquire);
      if (state >= TICKET_OFFSET) {
        EncodedKey key = _slots[i].key.load(std::memory_order_acquire);
        uint32_t ticket = state - TICKET_OFFSET;
        func(key, ticket);
      }
    }
  }

 private:
  size_t _capacity;
  size_t _mask;
  std::vector<Slot> _slots;
};

std::shared_ptr<Table> hash_multi_optimized(const std::shared_ptr<Table>& input) {
  const auto num_workers = CONFIG.num_workers;
  const auto chunk_count = input->chunk_count();

  // Column IDs
  const auto returnflag_col_id = input->column_id_by_name("l_returnflag");
  const auto linestatus_col_id = input->column_id_by_name("l_linestatus");
  const auto quantity_col_id = input->column_id_by_name("l_quantity");

  // Shared concurrent data structures
  // Capacity 16: power of 2, ~37.5% load factor for 6 groups
  auto hash_table = FolkloreHashTable(16);
  auto ticketer = FuzzyTicketer();

  // Per-worker partial aggregates (indexed by ticket)
  auto worker_partials = std::vector<std::vector<double>>(num_workers);

  // =========================================
  // PHASE 1: Parallel Ticketing + Local Updates
  // =========================================
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(num_workers);

  for (uint32_t worker_id = 0; worker_id < num_workers; ++worker_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, worker_id] {
      FuzzyTicketer::LocalState local_ticketer;
      auto& partials = worker_partials[worker_id];
      uint32_t max_ticket_seen = 0;

      // Round-robin chunk assignment
      for (auto chunk_id = ChunkID{worker_id};
           static_cast<ChunkID::base_type>(chunk_id) <
               static_cast<ChunkID::base_type>(chunk_count);
           chunk_id = ChunkID{static_cast<ChunkID::base_type>(chunk_id) + num_workers}) {
        const auto chunk = input->get_chunk(chunk_id);
        if (!chunk) continue;

        const auto chunk_size = chunk->size();
        const auto& rf_seg = *chunk->get_segment(returnflag_col_id);
        const auto& ls_seg = *chunk->get_segment(linestatus_col_id);
        const auto& qty_seg = *chunk->get_segment(quantity_col_id);

        // Materialize values for this chunk
        auto rf_vals = std::vector<char>{};
        auto ls_vals = std::vector<char>{};
        auto qty_vals = std::vector<float>{};
        rf_vals.reserve(chunk_size);
        ls_vals.reserve(chunk_size);
        qty_vals.reserve(chunk_size);

        segment_iterate<pmr_string>(rf_seg, [&](const auto& pos) {
          rf_vals.push_back(pos.is_null() ? '\0' : pos.value()[0]);
        });
        segment_iterate<pmr_string>(ls_seg, [&](const auto& pos) {
          ls_vals.push_back(pos.is_null() ? '\0' : pos.value()[0]);
        });
        segment_iterate<float>(qty_seg, [&](const auto& pos) {
          qty_vals.push_back(pos.is_null() ? 0.0f : pos.value());
        });

        // Process each row: get ticket, update local partial
        for (size_t i = 0; i < rf_vals.size(); ++i) {
          EncodedKey key = encode_key(rf_vals[i], ls_vals[i]);
          uint32_t ticket = hash_table.get_or_insert(key, ticketer, local_ticketer);

          // Ensure partials vector is large enough
          if (ticket >= partials.size()) {
            partials.resize(std::max<size_t>(ticket + 1, partials.size() * 2 + 8), 0.0);
          }

          partials[ticket] += static_cast<double>(qty_vals[i]);
          max_ticket_seen = std::max(max_ticket_seen, ticket);
        }
      }

      // Trim to actual size
      if (!partials.empty() && max_ticket_seen + 1 < partials.size()) {
        partials.resize(max_ticket_seen + 1);
      }
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  // =========================================
  // PHASE 2: Merge by Ticket
  // =========================================
  const uint32_t num_tickets = ticketer.max_ticket();
  auto final_sums = std::vector<double>(num_tickets, 0.0);

  // For small number of groups, single-threaded merge is efficient
  for (uint32_t ticket = 0; ticket < num_tickets; ++ticket) {
    for (uint32_t worker = 0; worker < num_workers; ++worker) {
      if (ticket < worker_partials[worker].size()) {
        final_sums[ticket] += worker_partials[worker][ticket];
      }
    }
  }

  // =========================================
  // PHASE 3: Materialization
  // =========================================
  auto column_definitions = TableColumnDefinitions{
      TableColumnDefinition{"l_returnflag", DataType::String, false},
      TableColumnDefinition{"l_linestatus", DataType::String, false},
      TableColumnDefinition{"sum_quantity", DataType::Double, false}};

  auto output = std::make_shared<Table>(column_definitions, TableType::Data);

  auto rf_out = pmr_vector<pmr_string>{};
  auto ls_out = pmr_vector<pmr_string>{};
  auto sum_out = pmr_vector<double>{};

  rf_out.reserve(num_tickets);
  ls_out.reserve(num_tickets);
  sum_out.reserve(num_tickets);

  // Iterate hash table to get key->ticket mapping
  hash_table.for_each_entry([&](EncodedKey encoded_key, uint32_t ticket) {
    auto [rf, ls] = decode_key(encoded_key);
    rf_out.push_back(pmr_string(1, rf));
    ls_out.push_back(pmr_string(1, ls));
    sum_out.push_back(final_sums[ticket]);
  });

  auto segments = Segments{};
  segments.push_back(std::make_shared<ValueSegment<pmr_string>>(std::move(rf_out)));
  segments.push_back(std::make_shared<ValueSegment<pmr_string>>(std::move(ls_out)));
  segments.push_back(std::make_shared<ValueSegment<double>>(std::move(sum_out)));

  output->append_chunk(segments);

  return output;
}


/**
 * SORT AGGREGATION IMPLEMENTATIONS
 */

std::shared_ptr<Table> sort_single_baseline(const std::shared_ptr<Table>& input) {
  using namespace hyrise;

  // 1. Prepare Input
  auto table_wrapper = std::make_shared<TableWrapper>(input);
  table_wrapper->execute();

  // 2. Define Columns
  // Group By: l_returnflag, l_linestatus
  // Aggregate: SUM(l_quantity)
  const auto returnflag_col_id = input->column_id_by_name("l_returnflag");
  const auto linestatus_col_id = input->column_id_by_name("l_linestatus");
  const auto quantity_col_id = input->column_id_by_name("l_quantity");

  auto quantity_expr = std::make_shared<PQPColumnExpression>(
      quantity_col_id,
      input->column_data_type(quantity_col_id),
      input->column_is_nullable(quantity_col_id),
      input->column_name(quantity_col_id));

  // 3. Define Aggregate Expressions
  auto sum_expr = expression_functional::sum_(quantity_expr);

  // 4. Create and Run Sort Aggregate
  auto aggregate_op = std::make_shared<AggregateSort>(
      table_wrapper,
      std::vector<std::shared_ptr<WindowFunctionExpression>>{sum_expr},
      std::vector<ColumnID>{returnflag_col_id, linestatus_col_id}
  );

  aggregate_op->execute();

  return std::const_pointer_cast<Table>(aggregate_op->get_output());
}

std::shared_ptr<Table> sort_single_baseline_hc(const std::shared_ptr<Table>& input) {
  using namespace hyrise;

  // 1. Prepare Input
  auto table_wrapper = std::make_shared<TableWrapper>(input);
  table_wrapper->execute();

  // 2. Define Columns
  // Group By: l_orderkey
  // Aggregate: SUM(l_quantity)
  const auto quantity_col_id = input->column_id_by_name("l_quantity");
  const auto orderkey_col = input->column_id_by_name("l_orderkey");

  auto quantity_expr = std::make_shared<PQPColumnExpression>(
      quantity_col_id,
      input->column_data_type(quantity_col_id),
      input->column_is_nullable(quantity_col_id),
      input->column_name(quantity_col_id));

  // 3. Define Aggregate Expressions
  auto sum_expr = expression_functional::sum_(quantity_expr);

  // 4. Create and Run Sort Aggregate
  auto aggregate_op = std::make_shared<AggregateSort>(
      table_wrapper,
      std::vector<std::shared_ptr<WindowFunctionExpression>>{sum_expr},
      std::vector<ColumnID>{orderkey_col}
  );

  aggregate_op->execute();

  return std::const_pointer_cast<Table>(aggregate_op->get_output());
}

std::shared_ptr<hyrise::Table> sort_single_optimized(const std::shared_ptr<hyrise::Table>& input) {
  using namespace hyrise;
  
  auto total_start = std::chrono::high_resolution_clock::now();

  const auto rf_col = input->column_id_by_name("l_returnflag");
  const auto ls_col = input->column_id_by_name("l_linestatus");
  const auto qty_col = input->column_id_by_name("l_quantity");

  // 1️⃣ OPTIMIZED STRUCT WITH PACKED KEY
  struct alignas(16) RowData {
    uint32_t key;  // Packed key (rf_id << 16 | ls_id)
    float qty;
    
    RowData(uint32_t k, float v) : key(k), qty(v) {}
    RowData() : key(0), qty(0.0f) {}
  };
  
  // --- TIMING: EXTRACTION ---
  auto extract_start = std::chrono::high_resolution_clock::now();
  
  const size_t total_rows = input->row_count();
  std::vector<RowData> rows;
  rows.reserve(total_rows);
  
  // Character mappings
  std::array<uint8_t, 256> char_to_id = {0};
  char_to_id['R'] = 0; char_to_id['A'] = 1; char_to_id['N'] = 2;
  char_to_id['F'] = 0; char_to_id['O'] = 1; 

  std::array<char, 3> rf_id_to_char = {'R', 'A', 'N'};
  std::array<char, 2> ls_id_to_char = {'F', 'O'};
  
  // BULK EXTRACTION
  for (ChunkID chunk_id{0}; chunk_id < input->chunk_count(); ++chunk_id) {
    const auto chunk = input->get_chunk(chunk_id);
    if (!chunk || chunk->size() == 0) continue;
    
    const auto& rf_seg = chunk->get_segment(rf_col);
    const auto& ls_seg = chunk->get_segment(ls_col);
    const auto& qty_seg = chunk->get_segment(qty_col);
    
    const auto chunk_size = chunk->size();
    const size_t unroll_factor = 8;
    
    for (size_t offset = 0; offset + unroll_factor <= chunk_size; offset += unroll_factor) {
      ChunkOffset co0 = static_cast<ChunkOffset>(offset);
      ChunkOffset co1 = static_cast<ChunkOffset>(offset + 1);
      ChunkOffset co2 = static_cast<ChunkOffset>(offset + 2);
      ChunkOffset co3 = static_cast<ChunkOffset>(offset + 3);
      ChunkOffset co4 = static_cast<ChunkOffset>(offset + 4);
      ChunkOffset co5 = static_cast<ChunkOffset>(offset + 5);
      ChunkOffset co6 = static_cast<ChunkOffset>(offset + 6);
      ChunkOffset co7 = static_cast<ChunkOffset>(offset + 7);
      
      uint16_t rf0 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co0])[0])];
      uint16_t ls0 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co0])[0])];
      rows.emplace_back((static_cast<uint32_t>(rf0) << 16) | ls0, boost::get<float>((*qty_seg)[co0]));
      
      uint16_t rf1 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co1])[0])];
      uint16_t ls1 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co1])[0])];
      rows.emplace_back((static_cast<uint32_t>(rf1) << 16) | ls1, boost::get<float>((*qty_seg)[co1]));
      
      uint16_t rf2 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co2])[0])];
      uint16_t ls2 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co2])[0])];
      rows.emplace_back((static_cast<uint32_t>(rf2) << 16) | ls2, boost::get<float>((*qty_seg)[co2]));
      
      uint16_t rf3 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co3])[0])];
      uint16_t ls3 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co3])[0])];
      rows.emplace_back((static_cast<uint32_t>(rf3) << 16) | ls3, boost::get<float>((*qty_seg)[co3]));
      
      uint16_t rf4 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co4])[0])];
      uint16_t ls4 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co4])[0])];
      rows.emplace_back((static_cast<uint32_t>(rf4) << 16) | ls4, boost::get<float>((*qty_seg)[co4]));
      
      uint16_t rf5 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co5])[0])];
      uint16_t ls5 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co5])[0])];
      rows.emplace_back((static_cast<uint32_t>(rf5) << 16) | ls5, boost::get<float>((*qty_seg)[co5]));
      
      uint16_t rf6 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co6])[0])];
      uint16_t ls6 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co6])[0])];
      rows.emplace_back((static_cast<uint32_t>(rf6) << 16) | ls6, boost::get<float>((*qty_seg)[co6]));
      
      uint16_t rf7 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co7])[0])];
      uint16_t ls7 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co7])[0])];
      rows.emplace_back((static_cast<uint32_t>(rf7) << 16) | ls7, boost::get<float>((*qty_seg)[co7]));
    }
    
    for (size_t offset = chunk_size - (chunk_size % unroll_factor); offset < chunk_size; ++offset) {
      ChunkOffset co = static_cast<ChunkOffset>(offset);
      uint16_t rf_id = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co])[0])];
      uint16_t ls_id = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co])[0])];
      rows.emplace_back((static_cast<uint32_t>(rf_id) << 16) | ls_id, 
                        boost::get<float>((*qty_seg)[co]));
    }
  }
  
  auto extract_end = std::chrono::high_resolution_clock::now();
  auto extract_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(extract_end - extract_start).count()) / 1000.0;
  
  // --- TIMING: COUNTING SORT ---
  auto sort_start = std::chrono::high_resolution_clock::now();
  
  // DIRECT COUNTING SORT
  auto get_dense_id = [](uint32_t key) constexpr -> uint8_t {
    return static_cast<uint8_t>(((key >> 16) & 0xFFFF) * 2 + (key & 0xFFFF));
  };

  std::array<size_t, 6> count = {0};
  for (const auto& row : rows) {
    count[get_dense_id(row.key)]++;
  }
  
  size_t total = 0;
  for (size_t i = 0; i < 6; ++i) {
    size_t old_count = count[i];
    count[i] = total;
    total += old_count;
  }
  
  std::vector<RowData> sorted(rows.size());
  for (const auto& row : rows) {
    uint8_t dense_id = get_dense_id(row.key);
    sorted[count[dense_id]++] = row;
  }
  
  rows.swap(sorted);
  
  auto sort_end = std::chrono::high_resolution_clock::now();
  auto sort_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(sort_end - sort_start).count()) / 1000.0;
  
  // --- TIMING: AGGREGATION ---
  auto agg_start = std::chrono::high_resolution_clock::now();
  
  struct AggResult { uint32_t key; double sum; };
  std::vector<AggResult> aggregates;
  aggregates.reserve(6);
  
  if (!rows.empty()) {
    size_t i = 0;
    const size_t n = rows.size();
    
    #ifdef __AVX2__
    while (i < n) {
      uint32_t current_key = rows[i].key;
      __m256d sum_vec = _mm256_setzero_pd();
      
      while (i + 3 < n && 
             rows[i].key == current_key && 
             rows[i+1].key == current_key && 
             rows[i+2].key == current_key && 
             rows[i+3].key == current_key) {
        
        __m128 float_vec = _mm_set_ps(rows[i+3].qty, rows[i+2].qty, 
                                       rows[i+1].qty, rows[i].qty);
        __m256d double_vec = _mm256_cvtps_pd(float_vec);
        sum_vec = _mm256_add_pd(sum_vec, double_vec);
        i += 4;
      }
      
      double simd_sum = 0.0;
      alignas(32) double temp[4];
      _mm256_store_pd(temp, sum_vec);
      simd_sum = temp[0] + temp[1] + temp[2] + temp[3];
      
      double scalar_sum = 0.0;
      while (i < n && rows[i].key == current_key) {
        scalar_sum += rows[i].qty;
        i++;
      }
      
      aggregates.push_back({current_key, simd_sum + scalar_sum});
    }
    #else
    uint32_t current_key = rows[0].key;
    double current_sum = rows[0].qty;
    for (size_t idx = 1; idx < n; ++idx) {
      if (rows[idx].key == current_key) {
        current_sum += rows[idx].qty;
      } else {
        aggregates.push_back({current_key, current_sum});
        current_key = rows[idx].key;
        current_sum = rows[idx].qty;
      }
    }
    aggregates.push_back({current_key, current_sum});
    #endif
  }
  
  auto agg_end = std::chrono::high_resolution_clock::now();
  auto agg_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(agg_end - agg_start).count()) / 1000.0;
  
  // --- TIMING: RESULT BUILDING ---
  auto result_start = std::chrono::high_resolution_clock::now();
  
  TableColumnDefinitions columns{
    {"l_returnflag", DataType::String, false},
    {"l_linestatus", DataType::String, false},
    {"sum_qty", DataType::Double, false}
  };
  
  auto result = std::make_shared<Table>(columns, TableType::Data);
  
  std::array<uint32_t, 6> sorted_keys = {
    (0 << 16) | 0, (0 << 16) | 1,
    (1 << 16) | 0, (1 << 16) | 1,
    (2 << 16) | 0, (2 << 16) | 1
  };
  
  static constexpr std::array<bool, 6> GROUP_EXISTS = {
    true, false, true, false, true, true
  };
  
  std::unordered_map<uint32_t, double> sum_map;
  for (const auto& agg : aggregates) sum_map[agg.key] = agg.sum;
  
  for (uint8_t i = 0; i < 6; ++i) {
    if (GROUP_EXISTS[i]) {
      uint32_t key = sorted_keys[i];
      uint16_t rf_id = (key >> 16) & 0xFFFF;
      uint16_t ls_id = key & 0xFFFF;
      
      result->append({
        AllTypeVariant(pmr_string(1, rf_id_to_char[rf_id])),
        AllTypeVariant(pmr_string(1, ls_id_to_char[ls_id])),
        AllTypeVariant(sum_map[key])
      });
    }
  }
  
  auto result_end = std::chrono::high_resolution_clock::now();
  auto result_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(result_end - result_start).count()) / 1000.0;
  auto total_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(result_end - total_start).count()) / 1000.0;
  
  // --- PRINT TIMING RESULTS ---
  std::cout << "\n══════════════════════════════════════════════\n";
  std::cout << "📊 SORT SINGLE OPTIMIZED - TIMING BREAKDOWN\n";
  std::cout << "══════════════════════════════════════════════\n";
  std::cout << "  ▸ Extraction:    " << std::setw(8) << std::fixed << std::setprecision(2) << extract_time << " ms  " 
            << std::setw(6) << std::setprecision(1) << (extract_time/total_time*100) << "%\n";
  std::cout << "  ▸ Counting Sort: " << std::setw(8) << std::fixed << std::setprecision(2) << sort_time << " ms  "
            << std::setw(6) << std::setprecision(1) << (sort_time/total_time*100) << "%\n";
  std::cout << "  ▸ Aggregation:   " << std::setw(8) << std::fixed << std::setprecision(2) << agg_time << " ms  "
            << std::setw(6) << std::setprecision(1) << (agg_time/total_time*100) << "%\n";
  std::cout << "  ▸ Result Build:  " << std::setw(8) << std::fixed << std::setprecision(2) << result_time << " ms  "
            << std::setw(6) << std::setprecision(1) << (result_time/total_time*100) << "%\n";
  std::cout << "  ──────────────────────────────────────────\n";
  std::cout << "  ▸ TOTAL:         " << std::setw(8) << std::fixed << std::setprecision(2) << total_time << " ms\n";
  std::cout << "══════════════════════════════════════════════\n";
  
  return result;
}


  std::shared_ptr<hyrise::Table> sort_single_optimized_hc(const std::shared_ptr<hyrise::Table>& input) {
  using namespace hyrise;
  
  auto total_start = std::chrono::high_resolution_clock::now();

  const auto orderkey_col = input->column_id_by_name("l_orderkey");
  const auto qty_col = input->column_id_by_name("l_quantity");

  // 1️⃣ OPTIMIZED STRUCT WITH DIRECT INTEGER KEY
  struct RowData {
    int32_t key;
    float qty;
    
    RowData(int32_t k, float v) : key(k), qty(v) {}
    RowData() : key(0), qty(0.0f) {}
  };
  
  // --- TIMING: EXTRACTION ---
  auto extract_start = std::chrono::high_resolution_clock::now();
  
  const size_t total_rows = input->row_count();
  std::vector<RowData> rows;
  rows.reserve(total_rows);
  
  // 2️⃣ BULK EXTRACTION - DIRECT INTEGER ACCESS
  for (ChunkID chunk_id{0}; chunk_id < input->chunk_count(); ++chunk_id) {
    const auto chunk = input->get_chunk(chunk_id);
    if (!chunk || chunk->size() == 0) continue;
    
    const auto& orderkey_seg = chunk->get_segment(orderkey_col);
    const auto& qty_seg = chunk->get_segment(qty_col);
    
    const auto chunk_size = chunk->size();
    const size_t unroll_factor = 8;
    
    for (size_t offset = 0; offset + unroll_factor <= chunk_size; offset += unroll_factor) {
      ChunkOffset co0 = static_cast<ChunkOffset>(offset);
      ChunkOffset co1 = static_cast<ChunkOffset>(offset + 1);
      ChunkOffset co2 = static_cast<ChunkOffset>(offset + 2);
      ChunkOffset co3 = static_cast<ChunkOffset>(offset + 3);
      ChunkOffset co4 = static_cast<ChunkOffset>(offset + 4);
      ChunkOffset co5 = static_cast<ChunkOffset>(offset + 5);
      ChunkOffset co6 = static_cast<ChunkOffset>(offset + 6);
      ChunkOffset co7 = static_cast<ChunkOffset>(offset + 7);
      
      rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co0]), boost::get<float>((*qty_seg)[co0]));
      rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co1]), boost::get<float>((*qty_seg)[co1]));
      rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co2]), boost::get<float>((*qty_seg)[co2]));
      rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co3]), boost::get<float>((*qty_seg)[co3]));
      rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co4]), boost::get<float>((*qty_seg)[co4]));
      rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co5]), boost::get<float>((*qty_seg)[co5]));
      rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co6]), boost::get<float>((*qty_seg)[co6]));
      rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co7]), boost::get<float>((*qty_seg)[co7]));
    }
    
    for (size_t offset = chunk_size - (chunk_size % unroll_factor); offset < chunk_size; ++offset) {
      ChunkOffset co = static_cast<ChunkOffset>(offset);
      rows.emplace_back(
        boost::get<int32_t>((*orderkey_seg)[co]),
        boost::get<float>((*qty_seg)[co])
      );
    }
  }
  
  auto extract_end = std::chrono::high_resolution_clock::now();
  double extract_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(extract_end - extract_start).count()) / 1000.0;
  
  // --- TIMING: RADIX SORT ---
  auto sort_start = std::chrono::high_resolution_clock::now();
  
  auto radix_sort = [](std::vector<RowData>& data) {
    if (data.empty()) return;
  
    const size_t n = data.size();
    std::vector<RowData> buffer(n);
    
    // PASS 1: Lower 16 bits (with sign bit flipped)
    {
      std::array<size_t, 65536> count = {0}; 
      
      for (size_t i = 0; i < n; ++i) {
        uint32_t unsigned_key = static_cast<uint32_t>(data[i].key) ^ 0x80000000;
        uint16_t lower = static_cast<uint16_t>(unsigned_key & 0xFFFF);
        count[lower]++;
      }
      
      uint32_t sum = 0;
      for (size_t i = 0; i < 65536; ++i) {
        uint32_t tmp = count[i];
        count[i] = sum;
        sum += tmp;
      }
      
      for (size_t i = 0; i < n; ++i) {
        uint32_t unsigned_key = static_cast<uint32_t>(data[i].key) ^ 0x80000000;
        uint16_t lower = static_cast<uint16_t>(unsigned_key & 0xFFFF);
        buffer[count[lower]++] = data[i];
      }
      
      data.swap(buffer);
    }
    
    // PASS 2: Higher 16 bits
    {
      std::array<uint32_t, 65536> count = {0};
      
      for (size_t i = 0; i < n; ++i) {
        uint32_t unsigned_key = static_cast<uint32_t>(data[i].key) ^ 0x80000000;
        uint16_t upper = static_cast<uint16_t>((unsigned_key >> 16) & 0xFFFF);
        count[upper]++;
      }
      
      uint32_t sum = 0;
      for (size_t i = 0; i < 65536; ++i) {
        uint32_t tmp = count[i];
        count[i] = sum;
        sum += tmp;
      }
      
      for (size_t i = 0; i < n; ++i) {
        uint32_t unsigned_key = static_cast<uint32_t>(data[i].key) ^ 0x80000000;
        uint16_t upper = static_cast<uint16_t>((unsigned_key >> 16) & 0xFFFF);
        buffer[count[upper]++] = data[i];
      }
      
      data.swap(buffer);
    }
  };
  
  radix_sort(rows);
  
  auto sort_end = std::chrono::high_resolution_clock::now();
  double sort_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(sort_end - sort_start).count()) / 1000.0;
  
  // --- TIMING: AGGREGATION ---
  auto agg_start = std::chrono::high_resolution_clock::now();
  
  struct AggResult {
    int32_t key;
    double sum;
  };
  
  std::vector<AggResult> aggregates;
  
  if (!rows.empty()) {
    aggregates.reserve(std::min(rows.size() / 10, size_t(1000000)));
    
    size_t i = 0;
    const size_t n = rows.size();
    
    #ifdef __AVX2__
    while (i < n) {
      int32_t current_key = rows[i].key;
      __m256d sum_vec = _mm256_setzero_pd();
      
      // Prefetch next cache line
      if (i + 64 < n) {
        __builtin_prefetch(&rows[i + 64], 0, 3);
      }
      
      while (i + 3 < n && 
             rows[i].key == current_key && 
             rows[i+1].key == current_key && 
             rows[i+2].key == current_key && 
             rows[i+3].key == current_key) {
        
        __m128 float_vec = _mm_set_ps(rows[i+3].qty, rows[i+2].qty, 
                                       rows[i+1].qty, rows[i].qty);
        __m256d double_vec = _mm256_cvtps_pd(float_vec);
        sum_vec = _mm256_add_pd(sum_vec, double_vec);
        
        i += 4;
      }
      
      double simd_sum = 0.0;
      alignas(32) double temp[4];
      _mm256_store_pd(temp, sum_vec);
      simd_sum = temp[0] + temp[1] + temp[2] + temp[3];
      
      double scalar_sum = 0.0;
      while (i < n && rows[i].key == current_key) {
        scalar_sum += rows[i].qty;
        i++;
      }
      
      aggregates.push_back({current_key, simd_sum + scalar_sum});
    }
    #else
    int32_t current_key = rows[0].key;
    double current_sum = static_cast<double>(rows[0].qty);
    
    for (size_t idx = 1; idx < n; ++idx) {
      if (rows[idx].key == current_key) {
        current_sum += rows[idx].qty;
      } else {
        aggregates.push_back({current_key, current_sum});
        current_key = rows[idx].key;
        current_sum = rows[idx].qty;
      }
    }
    aggregates.push_back({current_key, current_sum});
    #endif
  }
  
  auto agg_end = std::chrono::high_resolution_clock::now();
  double agg_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(agg_end - agg_start).count()) / 1000.0;
  
  // --- TIMING: RESULT BUILDING ---
  auto result_start = std::chrono::high_resolution_clock::now();
  
  TableColumnDefinitions columns{
  {"l_orderkey", DataType::Int, false},
  {"sum_qty", DataType::Double, false}
};

  // Create vectors with Hyrise's PMR allocator
  pmr_vector<int32_t> orderkeys;
  pmr_vector<double> sums;
  orderkeys.reserve(aggregates.size());
  sums.reserve(aggregates.size());

  // Batch all data into vectors - NO VIRTUAL CALLS, NO PER-ROW OVERHEAD!
  for (const auto& agg : aggregates) {
    orderkeys.push_back(agg.key);
    sums.push_back(agg.sum);
  }

  // Create segments directly from pmr_vectors - ONE ALLOCATION PER COLUMN!
  auto orderkey_segment = std::make_shared<ValueSegment<int32_t>>(std::move(orderkeys));
  auto sum_segment = std::make_shared<ValueSegment<double>>(std::move(sums));

  // Create a single chunk with both segments
  Segments segments;
  segments.push_back(orderkey_segment);
  segments.push_back(sum_segment);

  // Append the entire chunk at once - ONE OPERATION!
  auto result = std::make_shared<Table>(columns, TableType::Data);
  result->append_chunk(segments);
  
  auto result_end = std::chrono::high_resolution_clock::now();
  double result_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(result_end - result_start).count()) / 1000.0;
  double total_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(result_end - total_start).count()) / 1000.0;
  
  // --- PRINT TIMING RESULTS ---
  std::cout << "\n══════════════════════════════════════════════════════════════════\n";
  std::cout << "📊 SORT SINGLE OPTIMIZED HIGH CARDINALITY - TIMING BREAKDOWN\n";
  std::cout << "══════════════════════════════════════════════════════════════════\n";
  std::cout << "  ▸ Extraction:       " << std::setw(10) << std::fixed << std::setprecision(2) << extract_time << " ms  "
            << std::setw(6) << std::setprecision(1) << (extract_time/total_time*100) << "%\n";
  std::cout << "  ▸ Radix Sort:       " << std::setw(10) << std::fixed << std::setprecision(2) << sort_time << " ms  "
            << std::setw(6) << std::setprecision(1) << (sort_time/total_time*100) << "%\n";
  std::cout << "  ▸ Aggregation:      " << std::setw(10) << std::fixed << std::setprecision(2) << agg_time << " ms  "
            << std::setw(6) << std::setprecision(1) << (agg_time/total_time*100) << "%\n";
  std::cout << "  ▸ Result Building:  " << std::setw(10) << std::fixed << std::setprecision(2) << result_time << " ms  "
            << std::setw(6) << std::setprecision(1) << (result_time/total_time*100) << "%\n";
  std::cout << "  ──────────────────────────────────────────────────────────────\n";
  std::cout << "  ▸ TOTAL:            " << std::setw(10) << std::fixed << std::setprecision(2) << total_time << " ms\n";
  std::cout << "══════════════════════════════════════════════════════════════════\n\n";
  
  return result;
}

std::shared_ptr<Table> sort_multi_naive(const std::shared_ptr<Table>& input) {
  const auto rf_col  = input->column_id_by_name("l_returnflag");
  const auto ls_col  = input->column_id_by_name("l_linestatus");
  const auto qty_col = input->column_id_by_name("l_quantity");

  auto table_wrapper = std::make_shared<TableWrapper>(input);
  table_wrapper->execute();

  std::vector<SortColumnDefinition> sort_defs;
  sort_defs.emplace_back(rf_col, SortMode::AscendingNullsFirst);
  sort_defs.emplace_back(ls_col, SortMode::AscendingNullsFirst);

  auto sort_op = std::make_shared<SortForAggregate>(
    std::static_pointer_cast<const AbstractOperator>(table_wrapper),
    sort_defs,
    Chunk::DEFAULT_SIZE,
    SortForAggregate::ForceMaterialization::No
  );

  sort_op->execute();
  auto sorted = sort_op->get_output();

  // 1. First, collect all RowIDs for efficient parallel processing
  RowIDPosList all_row_ids;
  for (ChunkID chunk_id{0}; chunk_id < sorted->chunk_count(); ++chunk_id) {
    const auto chunk = sorted->get_chunk(chunk_id);
    for (ChunkOffset offset{0}; offset < chunk->size(); ++offset) {
      all_row_ids.emplace_back(chunk_id, offset);
    }
  }

  // 2. Determine number of workers and chunk size
  const auto num_workers = std::thread::hardware_concurrency();
  const auto chunk_size = (all_row_ids.size() + num_workers - 1) / num_workers;

  // 3. Thread-local partial aggregation results
  using GroupKey = std::pair<pmr_string, pmr_string>;
  struct ThreadResult {
    std::vector<GroupKey> groups;
    std::vector<double> sums;
  };
  
  std::vector<ThreadResult> thread_results(num_workers);
  
  // 4. Launch parallel tasks for partial aggregation
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(num_workers);
  
  for (size_t thread_idx = 0; thread_idx < num_workers; ++thread_idx) {
    const auto start_idx = thread_idx * chunk_size;
    const auto end_idx = std::min(start_idx + chunk_size, all_row_ids.size());
    
    if (start_idx >= end_idx) break;
    
    jobs.emplace_back(std::make_shared<JobTask>([&, thread_idx, start_idx, end_idx]() {
      auto& local_result = thread_results[thread_idx];
      auto& local_groups = local_result.groups;
      auto& local_sums = local_result.sums;
      
      pmr_string current_rf;
      pmr_string current_ls;
      double current_sum = 0.0;
      bool first = true;
      
      // Process assigned chunk
      for (size_t idx = start_idx; idx < end_idx; ++idx) {
        const auto& row_id = all_row_ids[idx];
        const auto chunk = sorted->get_chunk(row_id.chunk_id);
        
        const auto rf = boost::get<pmr_string>((*chunk->get_segment(rf_col))[row_id.chunk_offset]);
        const auto ls = boost::get<pmr_string>((*chunk->get_segment(ls_col))[row_id.chunk_offset]);
        const auto qty = boost::get<float>((*chunk->get_segment(qty_col))[row_id.chunk_offset]);
        
        if (first) {
          current_rf = rf;
          current_ls = ls;
          current_sum = qty;
          first = false;
        } else if (rf == current_rf && ls == current_ls) {
          current_sum += qty;
        } else {
          // Emit previous group
          local_groups.emplace_back(current_rf, current_ls);
          local_sums.push_back(current_sum);
          
          // Start new group
          current_rf = rf;
          current_ls = ls;
          current_sum = qty;
        }
      }
      
      // Emit last group in this chunk
      if (!first) {
        local_groups.emplace_back(current_rf, current_ls);
        local_sums.push_back(current_sum);
      }
    }));
  }
  
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  
  // 5. Merge partial results (this part is sequential but processes much less data)
  TableColumnDefinitions columns{
    {"l_returnflag", DataType::String, false},
    {"l_linestatus", DataType::String, false},
    {"sum_qty", DataType::Double, false}
  };
  
  auto result = std::make_shared<Table>(columns, TableType::Data);
  
  // Use a map for O(1) lookup during merge
  std::map<GroupKey, double> merged_results;
  
  // Merge all thread-local results
  for (const auto& thread_result : thread_results) {
    for (size_t i = 0; i < thread_result.groups.size(); ++i) {
      const auto& group = thread_result.groups[i];
      merged_results[group] += thread_result.sums[i];
    }
  }
  
  // 6. Write results in sorted order
  for (const auto& [group, sum] : merged_results) {
    result->append({group.first, group.second, sum});
  }
  
  return result;
}

std::shared_ptr<Table> sort_multi_naive_hc(const std::shared_ptr<Table>& input) {
  using namespace hyrise;
  
  const auto orderkey_col = input->column_id_by_name("l_orderkey");
  const auto qty_col = input->column_id_by_name("l_quantity");

  auto table_wrapper = std::make_shared<TableWrapper>(input);
  table_wrapper->execute();

  std::vector<SortColumnDefinition> sort_defs;
  sort_defs.emplace_back(orderkey_col, SortMode::AscendingNullsFirst);

  auto sort_op = std::make_shared<SortForAggregate>(
    std::static_pointer_cast<const AbstractOperator>(table_wrapper),
    sort_defs,
    Chunk::DEFAULT_SIZE,
    SortForAggregate::ForceMaterialization::No
  );

  sort_op->execute();
  auto sorted = sort_op->get_output();

  // 1. First, collect all RowIDs for efficient parallel processing
  RowIDPosList all_row_ids;
  for (ChunkID chunk_id{0}; chunk_id < sorted->chunk_count(); ++chunk_id) {
    const auto chunk = sorted->get_chunk(chunk_id);
    for (ChunkOffset offset{0}; offset < chunk->size(); ++offset) {
      all_row_ids.emplace_back(chunk_id, offset);
    }
  }

  // 2. Determine number of workers and chunk size
  const auto num_workers = std::thread::hardware_concurrency();
  const auto chunk_size = (all_row_ids.size() + num_workers - 1) / num_workers;

  // 3. Thread-local partial aggregation results
  struct ThreadResult {
    std::vector<int32_t> keys;  // l_orderkey values
    std::vector<double> sums;   // SUM(l_quantity)
  };
  
  std::vector<ThreadResult> thread_results(num_workers);
  
  // 4. Launch parallel tasks for partial aggregation
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(num_workers);
  
  for (size_t thread_idx = 0; thread_idx < num_workers; ++thread_idx) {
    const auto start_idx = thread_idx * chunk_size;
    const auto end_idx = std::min(start_idx + chunk_size, all_row_ids.size());
    
    if (start_idx >= end_idx) break;
    
    jobs.emplace_back(std::make_shared<JobTask>([&, thread_idx, start_idx, end_idx]() {
      auto& local_result = thread_results[thread_idx];
      auto& local_keys = local_result.keys;
      auto& local_sums = local_result.sums;
      
      int32_t current_key = 0;
      double current_sum = 0.0;
      bool first = true;
      
      // Process assigned chunk
      for (size_t idx = start_idx; idx < end_idx; ++idx) {
        const auto& row_id = all_row_ids[idx];
        const auto chunk = sorted->get_chunk(row_id.chunk_id);
        
        const auto orderkey = boost::get<int32_t>((*chunk->get_segment(orderkey_col))[row_id.chunk_offset]);
        const auto qty = boost::get<float>((*chunk->get_segment(qty_col))[row_id.chunk_offset]);
        
        if (first) {
          current_key = orderkey;
          current_sum = qty;
          first = false;
        } else if (orderkey == current_key) {
          current_sum += qty;
        } else {
          // Emit previous group
          local_keys.push_back(current_key);
          local_sums.push_back(current_sum);
          
          // Start new group
          current_key = orderkey;
          current_sum = qty;
        }
      }
      
      // Emit last group in this chunk
      if (!first) {
        local_keys.push_back(current_key);
        local_sums.push_back(current_sum);
      }
    }));
  }
  
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  
  // 5. Merge partial results (this part is sequential but processes much less data)
  TableColumnDefinitions columns{
    {"l_orderkey", DataType::Int, false},
    {"sum_qty", DataType::Double, false}
  };
  
  auto result = std::make_shared<Table>(columns, TableType::Data);
  
  // Use a map for O(log n) lookup during merge
  std::map<int32_t, double> merged_results;
  
  // Merge all thread-local results
  for (const auto& thread_result : thread_results) {
    for (size_t i = 0; i < thread_result.keys.size(); ++i) {
      merged_results[thread_result.keys[i]] += thread_result.sums[i];
    }
  }
  
  // 6. Write results in sorted order (map is already sorted by key)
  for (const auto& [key, sum] : merged_results) {
    result->append({
      AllTypeVariant(key),
      AllTypeVariant(sum)
    });
  }
  
  return result;
}

std::shared_ptr<hyrise::Table> sort_multi_optimized(const std::shared_ptr<hyrise::Table>& input) {
  using namespace hyrise;
  
  auto total_start = std::chrono::high_resolution_clock::now();

  const auto rf_col = input->column_id_by_name("l_returnflag");
  const auto ls_col = input->column_id_by_name("l_linestatus");
  const auto qty_col = input->column_id_by_name("l_quantity");

  struct alignas(16) RowData {
    uint32_t key;
    float qty;
    RowData(uint32_t k, float v) : key(k), qty(v) {}
    RowData() : key(0), qty(0.0f) {}
  };
  
  std::array<uint8_t, 256> char_to_id = {0};
  char_to_id['R'] = 0; char_to_id['A'] = 1; char_to_id['N'] = 2;
  char_to_id['F'] = 0; char_to_id['O'] = 1; 

  std::array<char, 3> rf_id_to_char = {'R', 'A', 'N'};
  std::array<char, 2> ls_id_to_char = {'F', 'O'};
  
  auto get_dense_id = [](uint32_t key) constexpr -> uint8_t {
    return static_cast<uint8_t>(((key >> 16) & 0xFFFF) * 2 + (key & 0xFFFF));
  };
  
  // --- TIMING: EXTRACTION + PER-CHUNK SORT ---
  auto extract_sort_start = std::chrono::high_resolution_clock::now();
  
  struct SortedChunk { std::vector<RowData> data; };
  std::vector<SortedChunk> sorted_chunks(input->chunk_count());
  std::atomic<size_t> total_rows{0};
  
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>();
  
  for (ChunkID chunk_id{0}; chunk_id < input->chunk_count(); ++chunk_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
      
      const auto chunk = input->get_chunk(chunk_id);
      if (!chunk || chunk->size() == 0) {
        sorted_chunks[chunk_id].data = {};
        return;
      }
      
      const auto& rf_seg = chunk->get_segment(rf_col);
      const auto& ls_seg = chunk->get_segment(ls_col);
      const auto& qty_seg = chunk->get_segment(qty_col);
      
      const auto chunk_size = chunk->size();
      std::vector<RowData> local_rows;
      local_rows.reserve(chunk_size);
      
      // EXTRACTION (same unrolled code)
      const size_t unroll_factor = 8;
      for (size_t offset = 0; offset + unroll_factor <= chunk_size; offset += unroll_factor) {
        ChunkOffset co0 = static_cast<ChunkOffset>(offset);
        ChunkOffset co1 = static_cast<ChunkOffset>(offset + 1);
        ChunkOffset co2 = static_cast<ChunkOffset>(offset + 2);
        ChunkOffset co3 = static_cast<ChunkOffset>(offset + 3);
        ChunkOffset co4 = static_cast<ChunkOffset>(offset + 4);
        ChunkOffset co5 = static_cast<ChunkOffset>(offset + 5);
        ChunkOffset co6 = static_cast<ChunkOffset>(offset + 6);
        ChunkOffset co7 = static_cast<ChunkOffset>(offset + 7);
        
        uint16_t rf0 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co0])[0])];
        uint16_t ls0 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co0])[0])];
        local_rows.emplace_back((static_cast<uint32_t>(rf0) << 16) | ls0, boost::get<float>((*qty_seg)[co0]));
        
        uint16_t rf1 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co1])[0])];
        uint16_t ls1 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co1])[0])];
        local_rows.emplace_back((static_cast<uint32_t>(rf1) << 16) | ls1, boost::get<float>((*qty_seg)[co1]));
        
        uint16_t rf2 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co2])[0])];
        uint16_t ls2 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co2])[0])];
        local_rows.emplace_back((static_cast<uint32_t>(rf2) << 16) | ls2, boost::get<float>((*qty_seg)[co2]));
        
        uint16_t rf3 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co3])[0])];
        uint16_t ls3 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co3])[0])];
        local_rows.emplace_back((static_cast<uint32_t>(rf3) << 16) | ls3, boost::get<float>((*qty_seg)[co3]));
        
        uint16_t rf4 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co4])[0])];
        uint16_t ls4 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co4])[0])];
        local_rows.emplace_back((static_cast<uint32_t>(rf4) << 16) | ls4, boost::get<float>((*qty_seg)[co4]));
        
        uint16_t rf5 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co5])[0])];
        uint16_t ls5 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co5])[0])];
        local_rows.emplace_back((static_cast<uint32_t>(rf5) << 16) | ls5, boost::get<float>((*qty_seg)[co5]));
        
        uint16_t rf6 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co6])[0])];
        uint16_t ls6 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co6])[0])];
        local_rows.emplace_back((static_cast<uint32_t>(rf6) << 16) | ls6, boost::get<float>((*qty_seg)[co6]));
        
        uint16_t rf7 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co7])[0])];
        uint16_t ls7 = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co7])[0])];
        local_rows.emplace_back((static_cast<uint32_t>(rf7) << 16) | ls7, boost::get<float>((*qty_seg)[co7]));
      }
      
      for (size_t offset = chunk_size - (chunk_size % unroll_factor); offset < chunk_size; ++offset) {
        ChunkOffset co = static_cast<ChunkOffset>(offset);
        uint16_t rf_id = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co])[0])];
        uint16_t ls_id = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co])[0])];
        local_rows.emplace_back((static_cast<uint32_t>(rf_id) << 16) | ls_id, 
                                boost::get<float>((*qty_seg)[co]));
      }
      
      // COUNTING SORT per chunk
      if (local_rows.empty()) {
        sorted_chunks[chunk_id].data = {};
        return;
      }
      
      std::array<size_t, 6> count = {0};
      for (const auto& row : local_rows) count[get_dense_id(row.key)]++;
      
      size_t total = 0;
      for (size_t i = 0; i < 6; ++i) {
        size_t old_count = count[i];
        count[i] = total;
        total += old_count;
      }
      
      std::vector<RowData> sorted_local(local_rows.size());
      for (const auto& row : local_rows) {
        uint8_t dense_id = get_dense_id(row.key);
        sorted_local[count[dense_id]++] = row;
      }
      
      sorted_chunks[chunk_id] = {std::move(sorted_local)};
      total_rows += chunk_size;
    }));
  }
  
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  
  auto extract_sort_end = std::chrono::high_resolution_clock::now();
  auto extract_sort_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(extract_sort_end - extract_sort_start).count()) / 1000.0;
  
  // --- TIMING: REDUCTION ---
auto reduce_start = std::chrono::high_resolution_clock::now();

std::vector<SortedChunk> non_empty_chunks;
non_empty_chunks.reserve(input->chunk_count());
for (auto& chunk : sorted_chunks) {
  if (!chunk.data.empty()) non_empty_chunks.push_back(std::move(chunk));
}

if (non_empty_chunks.empty()) {
  TableColumnDefinitions cols{{"l_returnflag", DataType::String, false},
                             {"l_linestatus", DataType::String, false},
                             {"sum_qty", DataType::Double, false}};
  return std::make_shared<Table>(cols, TableType::Data);
}

// 🔥 FIX 1: Declare reduction_tasks HERE!
std::vector<std::shared_ptr<AbstractTask>> reduction_tasks;
reduction_tasks.reserve(non_empty_chunks.size());

// Even FASTER reduction - per-group mutexes
std::array<double, 6> final_sums = {0};
std::array<std::mutex, 6> group_mutexes;  // One mutex per group!


for (auto& chunk : non_empty_chunks) {
  reduction_tasks.emplace_back(std::make_shared<JobTask>([&, chunk = std::move(chunk)]() {
    std::array<double, 6> local_sums = {0};
    
    for (const auto& row : chunk.data) {
      uint8_t dense_id = get_dense_id(row.key);
      local_sums[dense_id] += row.qty;
    }
    
    // Lock ONLY the groups that have data
    for (size_t i = 0; i < 6; ++i) {
      if (local_sums[i] != 0.0) {
        std::lock_guard<std::mutex> lock(group_mutexes[i]);
        final_sums[i] += local_sums[i];
      }
    }
  }));
}

for (auto& task : reduction_tasks) task->schedule();
Hyrise::get().scheduler()->wait_for_tasks(reduction_tasks);

auto reduce_end = std::chrono::high_resolution_clock::now();
auto reduce_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(reduce_end - reduce_start).count()) / 1000.0;
  
  // --- TIMING: RESULT BUILDING ---
  auto result_start = std::chrono::high_resolution_clock::now();
  
  TableColumnDefinitions columns{
    {"l_returnflag", DataType::String, false},
    {"l_linestatus", DataType::String, false},
    {"sum_qty", DataType::Double, false}
  };
  
  auto result = std::make_shared<Table>(columns, TableType::Data);
  
  std::array<uint32_t, 6> sorted_keys = {
    (0 << 16) | 0, (0 << 16) | 1,
    (1 << 16) | 0, (1 << 16) | 1,
    (2 << 16) | 0, (2 << 16) | 1
  };
  
  static constexpr std::array<bool, 6> GROUP_EXISTS = {
    true, false, true, false, true, true
  };
  
  // No more atomic loads - just direct reads!
  for (uint8_t i = 0; i < 6; ++i) {
    if (GROUP_EXISTS[i]) {
      uint32_t key = sorted_keys[i];
      double sum = final_sums[i];  // Direct read, no atomic!
      uint16_t rf_id = (key >> 16) & 0xFFFF;
      uint16_t ls_id = key & 0xFFFF;
      
      result->append({
        AllTypeVariant(pmr_string(1, rf_id_to_char[rf_id])),
        AllTypeVariant(pmr_string(1, ls_id_to_char[ls_id])),
        AllTypeVariant(sum)
      });
    }
  }
  
  auto result_end = std::chrono::high_resolution_clock::now();
  auto result_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(result_end - result_start).count()) / 1000.0;
  auto total_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(result_end - total_start).count()) / 1000.0;
  
  // --- PRINT TIMING RESULTS ---
  std::cout << "\n══════════════════════════════════════════════\n";
  std::cout << "📊 SORT MULTI OPTIMIZED (32 workers) - TIMING BREAKDOWN\n";
  std::cout << "══════════════════════════════════════════════\n";
  std::cout << "  ▸ Extract + Per-chunk Sort: " << std::setw(8) << std::fixed << std::setprecision(2) << extract_sort_time << " ms  "
            << std::setw(6) << std::setprecision(1) << (extract_sort_time/total_time*100) << "%\n";
  std::cout << "  ▸ Parallel Reduction:      " << std::setw(8) << std::fixed << std::setprecision(2) << reduce_time << " ms  "
            << std::setw(6) << std::setprecision(1) << (reduce_time/total_time*100) << "%\n";
  std::cout << "  ▸ Result Building:         " << std::setw(8) << std::fixed << std::setprecision(2) << result_time << " ms  "
            << std::setw(6) << std::setprecision(1) << (result_time/total_time*100) << "%\n";
  std::cout << "  ──────────────────────────────────────────\n";
  std::cout << "  ▸ TOTAL:                   " << std::setw(8) << std::fixed << std::setprecision(2) << total_time << " ms\n";
  std::cout << "══════════════════════════════════════════════\n";
  
  return result;
}

  std::shared_ptr<hyrise::Table> sort_multi_optimized_hc(const std::shared_ptr<hyrise::Table>& input) {
  using namespace hyrise;
  
  auto total_start = std::chrono::high_resolution_clock::now();

  const auto orderkey_col = input->column_id_by_name("l_orderkey");
  const auto qty_col = input->column_id_by_name("l_quantity");

  // 1️⃣ OPTIMIZED STRUCT WITH DIRECT INTEGER KEY
  struct RowData {
    int32_t key;
    float qty;
    
    RowData(int32_t k, float v) : key(k), qty(v) {}
    RowData() : key(0), qty(0.0f) {}
  };
  
  // ───────────────────────────────────────────────────────────────────────
  // PHASE 1: PARALLEL EXTRACTION + PER-CHUNK RADIX SORT
  // ───────────────────────────────────────────────────────────────────────
  
  auto extract_sort_start = std::chrono::high_resolution_clock::now();
  
  struct SortedChunk {
    std::vector<RowData> data;
  };
  
  std::vector<SortedChunk> sorted_chunks(input->chunk_count());
  std::atomic<size_t> total_rows{0};
  
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>();
  
  for (ChunkID chunk_id{0}; chunk_id < input->chunk_count(); ++chunk_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
      const auto chunk = input->get_chunk(chunk_id);
      if (!chunk || chunk->size() == 0) {
        sorted_chunks[chunk_id].data = {};
        return;
      }
      
      const auto& orderkey_seg = chunk->get_segment(orderkey_col);
      const auto& qty_seg = chunk->get_segment(qty_col);
      
      const auto chunk_size = chunk->size();
      std::vector<RowData> local_rows;
      local_rows.reserve(chunk_size);
      
      // ----- EXTRACTION (unrolled) -----
      const size_t unroll_factor = 8;
      
      for (size_t offset = 0; offset + unroll_factor <= chunk_size; offset += unroll_factor) {
        ChunkOffset co0 = static_cast<ChunkOffset>(offset);
        ChunkOffset co1 = static_cast<ChunkOffset>(offset + 1);
        ChunkOffset co2 = static_cast<ChunkOffset>(offset + 2);
        ChunkOffset co3 = static_cast<ChunkOffset>(offset + 3);
        ChunkOffset co4 = static_cast<ChunkOffset>(offset + 4);
        ChunkOffset co5 = static_cast<ChunkOffset>(offset + 5);
        ChunkOffset co6 = static_cast<ChunkOffset>(offset + 6);
        ChunkOffset co7 = static_cast<ChunkOffset>(offset + 7);
        
        local_rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co0]), boost::get<float>((*qty_seg)[co0]));
        local_rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co1]), boost::get<float>((*qty_seg)[co1]));
        local_rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co2]), boost::get<float>((*qty_seg)[co2]));
        local_rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co3]), boost::get<float>((*qty_seg)[co3]));
        local_rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co4]), boost::get<float>((*qty_seg)[co4]));
        local_rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co5]), boost::get<float>((*qty_seg)[co5]));
        local_rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co6]), boost::get<float>((*qty_seg)[co6]));
        local_rows.emplace_back(boost::get<int32_t>((*orderkey_seg)[co7]), boost::get<float>((*qty_seg)[co7]));
      }
      
      for (size_t offset = chunk_size - (chunk_size % unroll_factor); offset < chunk_size; ++offset) {
        ChunkOffset co = static_cast<ChunkOffset>(offset);
        local_rows.emplace_back(
          boost::get<int32_t>((*orderkey_seg)[co]),
          boost::get<float>((*qty_seg)[co])
        );
      }
      
      // ----- PER-CHUNK RADIX SORT -----
      if (!local_rows.empty()) {
        std::vector<RowData> buffer(local_rows.size());
        
        // PASS 1: Lower 16 bits
        {
          std::array<uint32_t, 65536> count = {0};
          for (const auto& row : local_rows) {
            uint32_t unsigned_key = static_cast<uint32_t>(row.key) ^ 0x80000000;
            uint16_t lower = static_cast<uint16_t>(unsigned_key & 0xFFFF);
            count[lower]++;
          }
          
          uint32_t sum = 0;
          for (size_t i = 0; i < 65536; ++i) {
            uint32_t tmp = count[i];
            count[i] = sum;
            sum += tmp;
          }
          
          for (const auto& row : local_rows) {
            uint32_t unsigned_key = static_cast<uint32_t>(row.key) ^ 0x80000000;
            uint16_t lower = static_cast<uint16_t>(unsigned_key & 0xFFFF);
            buffer[count[lower]++] = row;
          }
          local_rows.swap(buffer);
        }
        
        // PASS 2: Higher 16 bits
        {
          std::array<uint32_t, 65536> count = {0};
          for (const auto& row : local_rows) {
            uint32_t unsigned_key = static_cast<uint32_t>(row.key) ^ 0x80000000;
            uint16_t upper = static_cast<uint16_t>((unsigned_key >> 16) & 0xFFFF);
            count[upper]++;
          }
          
          uint32_t sum = 0;
          for (size_t i = 0; i < 65536; ++i) {
            uint32_t tmp = count[i];
            count[i] = sum;
            sum += tmp;
          }
          
          for (const auto& row : local_rows) {
            uint32_t unsigned_key = static_cast<uint32_t>(row.key) ^ 0x80000000;
            uint16_t upper = static_cast<uint16_t>((unsigned_key >> 16) & 0xFFFF);
            buffer[count[upper]++] = row;
          }
          local_rows.swap(buffer);
        }
      }
      
      sorted_chunks[chunk_id] = {std::move(local_rows)};
      total_rows += chunk_size;
    }));
  }
  
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  
  auto extract_sort_end = std::chrono::high_resolution_clock::now();
  double extract_sort_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(extract_sort_end - extract_sort_start).count()) / 1000.0;
  
  // ───────────────────────────────────────────────────────────────────────
  // PHASE 2: REMOVE EMPTY CHUNKS
  // ───────────────────────────────────────────────────────────────────────
  
  auto remove_start = std::chrono::high_resolution_clock::now();
  
  std::vector<SortedChunk> non_empty_chunks;
  non_empty_chunks.reserve(input->chunk_count());
  for (auto& chunk : sorted_chunks) {
    if (!chunk.data.empty()) {
      non_empty_chunks.push_back(std::move(chunk));
    }
  }
  
  if (non_empty_chunks.empty()) {
    TableColumnDefinitions columns{
      {"l_orderkey", DataType::Int, false},
      {"sum_qty", DataType::Double, false}
    };
    return std::make_shared<Table>(columns, TableType::Data);
  }
  
  auto remove_end = std::chrono::high_resolution_clock::now();
  double remove_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(remove_end - remove_start).count()) / 1000.0;
  
  // ───────────────────────────────────────────────────────────────────────
  // PHASE 3: SAMPLE-BASED PARTITIONING - NO MERGE!
  // ───────────────────────────────────────────────────────────────────────
  
  auto partition_start = std::chrono::high_resolution_clock::now();
  
  const size_t num_partitions = std::thread::hardware_concurrency();
  
  // 1. Collect samples from all chunks (O(chunks))
  std::vector<int32_t> samples;
  samples.reserve(non_empty_chunks.size() * 100);
  
  for (const auto& chunk : non_empty_chunks) {
    if (chunk.data.empty()) continue;
    // Sample every 100th row
    for (size_t i = 0; i < chunk.data.size(); i += 100) {
      samples.push_back(chunk.data[i].key);
    }
  }
  
  // 2. Find quantile boundaries
  std::sort(samples.begin(), samples.end());
  std::vector<int32_t> boundaries;
  boundaries.push_back(std::numeric_limits<int32_t>::min());
  
  for (size_t i = 1; i < num_partitions; ++i) {
    size_t idx = (samples.size() * i) / num_partitions;
    boundaries.push_back(samples[idx]);
  }
  boundaries.push_back(std::numeric_limits<int32_t>::max());
  
  // 3. Parallel partition chunks into final buckets
  std::vector<std::vector<RowData>> final_partitions(num_partitions);
  std::array<std::mutex, 64> partition_mutexes;  // Enough for up to 64 partitions
  std::vector<std::shared_ptr<AbstractTask>> partition_tasks;
  
  for (auto& chunk : non_empty_chunks) {
    partition_tasks.emplace_back(std::make_shared<JobTask>([&, chunk = std::move(chunk)]() {
      // Thread-local buffers to avoid locking
      thread_local std::vector<RowData> local_buffers[64];
      
      // For each row, find its partition via binary search
      for (const auto& row : chunk.data) {
        auto it = std::upper_bound(boundaries.begin(), boundaries.end(), row.key);
        size_t partition_idx = std::distance(boundaries.begin(), it) - 1;
        local_buffers[partition_idx].push_back(row);
      }
      
      // Merge thread-local buffers into final partitions
      for (size_t p = 0; p < num_partitions; ++p) {
        if (!local_buffers[p].empty()) {
          std::lock_guard<std::mutex> lock(partition_mutexes[p]);
          final_partitions[p].insert(final_partitions[p].end(),
                                   std::make_move_iterator(local_buffers[p].begin()),
                                   std::make_move_iterator(local_buffers[p].end()));
          local_buffers[p].clear();
        }
      }
    }));
  }
  
  for (auto& task : partition_tasks) task->schedule();
  Hyrise::get().scheduler()->wait_for_tasks(partition_tasks);
  
  // 4. Sort each partition in parallel (they're partially sorted per chunk!)
  std::vector<std::shared_ptr<AbstractTask>> sort_tasks;
  for (size_t p = 0; p < num_partitions; ++p) {
    sort_tasks.emplace_back(std::make_shared<JobTask>([&, p]() {
      // Each partition is small and unsorted - use pdqsort
      boost::sort::pdqsort(final_partitions[p].begin(), final_partitions[p].end(),
          [](const RowData& a, const RowData& b) { return a.key < b.key; });
    }));
  }
  
  for (auto& task : sort_tasks) task->schedule();
  Hyrise::get().scheduler()->wait_for_tasks(sort_tasks);
  
  // 5. Concatenate partitions - NO FINAL MERGE NEEDED!
  std::vector<RowData> final_sorted;
  size_t total_size = 0;
  for (const auto& p : final_partitions) total_size += p.size();
  final_sorted.reserve(total_size);
  for (auto& p : final_partitions) {
    final_sorted.insert(final_sorted.end(),
                       std::make_move_iterator(p.begin()),
                       std::make_move_iterator(p.end()));
  }
  
  auto partition_end = std::chrono::high_resolution_clock::now();
  double partition_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(partition_end - partition_start).count()) / 1000.0;
  
  // ───────────────────────────────────────────────────────────────────────
  // PHASE 4: PARALLEL AGGREGATION - PERFECT KEY BOUNDARY PARTITIONING!
  // ───────────────────────────────────────────────────────────────────────
  
  auto agg_start = std::chrono::high_resolution_clock::now();
  
  struct AggResult {
    int32_t key;
    double sum;
  };
  std::vector<AggResult> aggregates;
  
  if (!final_sorted.empty()) {
    const size_t num_tasks = std::thread::hardware_concurrency();
    
    // Find perfect split points at key boundaries
    std::vector<size_t> split_points;
    split_points.push_back(0);
    
    size_t target_rows_per_task = final_sorted.size() / num_tasks;
    
    for (size_t t = 1; t < num_tasks; ++t) {
      size_t target = t * target_rows_per_task;
      if (target >= final_sorted.size()) break;
      
      // Find the next key boundary
      while (target < final_sorted.size() && 
             target > 0 && 
             final_sorted[target].key == final_sorted[target - 1].key) {
        target++;
      }
      if (target < final_sorted.size()) {
        split_points.push_back(target);
      }
    }
    split_points.push_back(final_sorted.size());
    
    // Thread-local aggregation on KEY-ALIGNED ranges
    std::vector<std::vector<AggResult>> local_results(split_points.size() - 1);
    std::vector<std::shared_ptr<AbstractTask>> agg_tasks;
    
    for (size_t t = 0; t < split_points.size() - 1; ++t) {
      size_t start = split_points[t];
      size_t end = split_points[t + 1];
      
      agg_tasks.emplace_back(std::make_shared<JobTask>([&, t, start, end]() {
        std::vector<AggResult> local;
        local.reserve((end - start) / 10);
        
        size_t i = start;
        while (i < end) {
          int32_t current_key = final_sorted[i].key;
          double sum = 0.0;
          
          while (i < end && final_sorted[i].key == current_key) {
            sum += final_sorted[i].qty;
            i++;
          }
          local.push_back({current_key, sum});
        }
        local_results[t] = std::move(local);
      }));
    }
    
    for (auto& task : agg_tasks) task->schedule();
    Hyrise::get().scheduler()->wait_for_tasks(agg_tasks);
    
    // Merge local results (they're already sorted by key!)
    size_t total_agg_size = 0;
    for (const auto& local : local_results) total_agg_size += local.size();
    aggregates.reserve(total_agg_size);
    
    // Multi-way merge of sorted vectors
    struct HeapEntry {
      AggResult agg;
      size_t task_idx;
      size_t pos;
      bool operator<(const HeapEntry& other) const { return agg.key > other.agg.key; }
    };
    
    std::priority_queue<HeapEntry> pq;
    for (size_t i = 0; i < local_results.size(); ++i) {
      if (!local_results[i].empty()) {
        pq.push({local_results[i][0], i, 0});
      }
    }
    
    while (!pq.empty()) {
      HeapEntry entry = pq.top();
      pq.pop();
      aggregates.push_back(entry.agg);
      
      if (entry.pos + 1 < local_results[entry.task_idx].size()) {
        pq.push({local_results[entry.task_idx][entry.pos + 1], entry.task_idx, entry.pos + 1});
      }
    }
  }
  
  auto agg_end = std::chrono::high_resolution_clock::now();
  double agg_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(agg_end - agg_start).count()) / 1000.0;
  
  // ───────────────────────────────────────────────────────────────────────
  // PHASE 5: BATCHED RESULT BUILDING
  // ───────────────────────────────────────────────────────────────────────
  
  auto result_start = std::chrono::high_resolution_clock::now();
  
  TableColumnDefinitions columns{
    {"l_orderkey", DataType::Int, false},
    {"sum_qty", DataType::Double, false}
  };
  
  pmr_vector<int32_t> orderkeys;
  pmr_vector<double> sums;
  orderkeys.reserve(aggregates.size());
  sums.reserve(aggregates.size());
  
  for (const auto& agg : aggregates) {
    orderkeys.push_back(agg.key);
    sums.push_back(agg.sum);
  }
  
  auto orderkey_segment = std::make_shared<ValueSegment<int32_t>>(std::move(orderkeys));
  auto sum_segment = std::make_shared<ValueSegment<double>>(std::move(sums));
  
  Segments segments;
  segments.push_back(orderkey_segment);
  segments.push_back(sum_segment);
  
  auto result = std::make_shared<Table>(columns, TableType::Data);
  result->append_chunk(segments);
  
  auto result_end = std::chrono::high_resolution_clock::now();
  double result_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(result_end - result_start).count()) / 1000.0;
  double total_time = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(result_end - total_start).count()) / 1000.0;
  
  // --- PRINT TIMING RESULTS ---
  std::cout << "\n══════════════════════════════════════════════════════════════════\n";
  std::cout << "📊 SORT MULTI OPTIMIZED HIGH CARDINALITY (32 workers) - TIMING BREAKDOWN\n";
  std::cout << "══════════════════════════════════════════════════════════════════\n";
  std::cout << "  ▸ Extract + Radix Sort: " << std::setw(10) << std::fixed << std::setprecision(2) << extract_sort_time << " ms  "
            << std::setw(6) << std::setprecision(1) << (extract_sort_time/total_time*100) << "%\n";
  std::cout << "  ▸ Remove Empty Chunks:  " << std::setw(10) << std::fixed << std::setprecision(2) << remove_time << " ms  "
            << std::setw(6) << std::setprecision(1) << (remove_time/total_time*100) << "%\n";
  std::cout << "  ▸ Sample Partitioning:  " << std::setw(10) << std::fixed << std::setprecision(2) << partition_time << " ms  "
            << std::setw(6) << std::setprecision(1) << (partition_time/total_time*100) << "%\n";
  std::cout << "  ▸ Parallel Aggregation: " << std::setw(10) << std::fixed << std::setprecision(2) << agg_time << " ms  "
            << std::setw(6) << std::setprecision(1) << (agg_time/total_time*100) << "%\n";
  std::cout << "  ▸ Result Building:      " << std::setw(10) << std::fixed << std::setprecision(2) << result_time << " ms  "
            << std::setw(6) << std::setprecision(1) << (result_time/total_time*100) << "%\n";
  std::cout << "  ──────────────────────────────────────────────────────────────\n";
  std::cout << "  ▸ TOTAL:                " << std::setw(10) << std::fixed << std::setprecision(2) << total_time << " ms\n";
  std::cout << "══════════════════════════════════════════════════════════════════\n\n";
  
  return result;
}

/**
 * HELPER: Compare two aggregation result tables
 * Since aggregation results can have rows in different orders, we convert to sets for comparison.
 */
bool verify_tables_equal(const Table& actual, const Table& expected) {
  // Check row counts
  if (actual.row_count() != expected.row_count()) {
    std::cerr << "    [VALIDATION FAILED] Row count mismatch: "
              << actual.row_count() << " vs " << expected.row_count() << std::endl;
    return false;
  }

  // Check column counts
  if (actual.column_count() != expected.column_count()) {
    std::cerr << "    [VALIDATION FAILED] Column count mismatch: "
              << actual.column_count() << " vs " << expected.column_count() << std::endl;
    return false;
  }

  // Convert both tables to set of rows (represented as strings for simplicity)
  auto table_to_set = [](const Table& table) {
    std::unordered_set<std::string> row_set;
    const auto chunk_count = table.chunk_count();

    // Iterate through all chunks
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = table.get_chunk(chunk_id);
      if (!chunk) continue;

      const auto chunk_size = chunk->size();
      const auto col_count = table.column_count();

      // Build vector of column values for each column
      std::vector<std::vector<std::string>> column_values(col_count);

      for (auto col_idx = ColumnID{0}; col_idx < col_count; ++col_idx) {
        const auto& segment = *chunk->get_segment(col_idx);
        const auto data_type = table.column_data_type(col_idx);

        column_values[col_idx].reserve(chunk_size);

        if (data_type == DataType::String) {
          segment_iterate<pmr_string>(segment, [&](const auto& pos) {
            column_values[col_idx].push_back(pos.is_null() ? "NULL" : std::string(pos.value()));
          });
        } else if (data_type == DataType::Double) {
          segment_iterate<double>(segment, [&](const auto& pos) {
            column_values[col_idx].push_back(pos.is_null() ? "NULL" : std::to_string(pos.value()));
          });
        } else if (data_type == DataType::Float) {
          segment_iterate<float>(segment, [&](const auto& pos) {
            column_values[col_idx].push_back(pos.is_null() ? "NULL" : std::to_string(pos.value()));
          });
        }
      }

      // Combine values for each row
      for (size_t row_in_chunk = 0; row_in_chunk < chunk_size; ++row_in_chunk) {
        std::string row_str;
        for (auto col_idx = ColumnID{0}; col_idx < col_count; ++col_idx) {
          row_str += column_values[col_idx][row_in_chunk] + "|";
        }
        row_set.insert(row_str);
      }
    }
    return row_set;
  };

  auto actual_rows = table_to_set(actual);
  auto expected_rows = table_to_set(expected);

  if (actual_rows != expected_rows) {
    std::cerr << "    [VALIDATION FAILED] Row content mismatch" << std::endl;
    return false;
  }

  return true;
}

bool verify_tables_equal_hc(const Table& actual, const Table& expected) {
  // Quick checks
  if (actual.row_count() != expected.row_count()) {
    std::cerr << "    [VALIDATION FAILED] Row count mismatch: "
              << actual.row_count() << " vs " << expected.row_count() << std::endl;
    return false;
  }

  if (actual.column_count() != expected.column_count()) {
    std::cerr << "    [VALIDATION FAILED] Column count mismatch" << std::endl;
    return false;
  }

  // For high cardinality, sort both tables and compare row by row
  std::vector<std::pair<int32_t, double>> actual_rows;
  std::vector<std::pair<int32_t, double>> expected_rows;
  
  actual_rows.reserve(actual.row_count());
  expected_rows.reserve(expected.row_count());
  
  // Extract actual table
  for (auto chunk_id = ChunkID{0}; chunk_id < actual.chunk_count(); ++chunk_id) {
    const auto chunk = actual.get_chunk(chunk_id);
    if (!chunk) continue;
    
    const auto& key_seg = *chunk->get_segment(ColumnID{0});
    const auto& sum_seg = *chunk->get_segment(ColumnID{1});
    
    for (ChunkOffset offset{0}; offset < chunk->size(); ++offset) {
      int32_t key = boost::get<int32_t>(key_seg[offset]);
      double sum = boost::get<double>(sum_seg[offset]);
      actual_rows.emplace_back(key, sum);
    }
  }
  
  // Extract expected table
  for (auto chunk_id = ChunkID{0}; chunk_id < expected.chunk_count(); ++chunk_id) {
    const auto chunk = expected.get_chunk(chunk_id);
    if (!chunk) continue;
    
    const auto& key_seg = *chunk->get_segment(ColumnID{0});
    const auto& sum_seg = *chunk->get_segment(ColumnID{1});
    
    for (ChunkOffset offset{0}; offset < chunk->size(); ++offset) {
      int32_t key = boost::get<int32_t>(key_seg[offset]);
      double sum = boost::get<double>(sum_seg[offset]);
      expected_rows.emplace_back(key, sum);
    }
  }
  
  // Sort both by key
  std::sort(actual_rows.begin(), actual_rows.end());
  std::sort(expected_rows.begin(), expected_rows.end());
  
  // Compare row by row
  const double EPSILON = 0.001;
  for (size_t i = 0; i < actual_rows.size(); ++i) {
    if (actual_rows[i].first != expected_rows[i].first) {
      std::cerr << "    [VALIDATION FAILED] Key mismatch at row " << i << ": "
                << actual_rows[i].first << " vs " << expected_rows[i].first << std::endl;
      return false;
    }
    if (std::abs(actual_rows[i].second - expected_rows[i].second) > EPSILON) {
      std::cerr << "    [VALIDATION FAILED] Sum mismatch at key " << actual_rows[i].first << ": "
                << actual_rows[i].second << " vs " << expected_rows[i].second << std::endl;
      return false;
    }
  }
  
  return true;
}


/**
 * BENCHMARK DRIVER - Low Cardinality
 */
std::shared_ptr<Table> run_algorithm(const std::string& name,
                                     std::function<std::shared_ptr<Table>(const std::shared_ptr<Table>&)> func,
                                     const std::shared_ptr<Table>& input,
                                     const std::shared_ptr<Table>& expected_result) {
  const auto num_iterations = CONFIG.num_iterations;

  std::cout << "  Running " << name << " (" << num_iterations << " iterations)... " << std::flush;

  auto durations = std::vector<int64_t>{};
  durations.reserve(num_iterations);
  std::shared_ptr<Table> result = nullptr;

  for (uint32_t i = 0; i < num_iterations; ++i) {
    const auto start = std::chrono::high_resolution_clock::now();
    result = func(input);
    Hyrise::get().scheduler()->wait_for_all_tasks();
    const auto end = std::chrono::high_resolution_clock::now();

    const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    durations.push_back(duration);

    if (i == 0 && expected_result && result) {
      if (!verify_tables_equal(*result, *expected_result)) {
        std::cout << "FAILED VALIDATION!" << std::endl;
        return result;
      } else {
        std::cout << "[VALIDATED] " << std::flush;
      }
    }
  }

  // Calculate statistics
  const auto min = *std::min_element(durations.begin(), durations.end());
  const auto max = *std::max_element(durations.begin(), durations.end());

  double avg;
  if (num_iterations >= 3) {
    auto sorted_durations = durations;
    std::sort(sorted_durations.begin(), sorted_durations.end());

    int64_t sum = 0;
    for (size_t i = 1; i < sorted_durations.size() - 1; ++i) {
      sum += sorted_durations[i];
    }
    avg = static_cast<double>(sum) / static_cast<double>(num_iterations - 2);
  } else {
    const auto sum = std::accumulate(durations.begin(), durations.end(), int64_t{0});
    avg = static_cast<double>(sum) / static_cast<double>(num_iterations);
  }

  std::cout << "Done." << std::endl;
  std::cout << "    Avg: " << std::fixed << std::setprecision(1) << avg << " ms";
  if (num_iterations >= 3) {
    std::cout << " (trimmed)";
  }
  std::cout << " | Min: " << min << " ms"
            << " | Max: " << max << " ms" << std::endl;

  return result;
}

/**
 * BENCHMARK DRIVER - High Cardinality
 */
std::shared_ptr<Table> run_algorithm_hc(const std::string& name,
                                        std::function<std::shared_ptr<Table>(const std::shared_ptr<Table>&)> func,
                                        const std::shared_ptr<Table>& input,
                                        const std::shared_ptr<Table>& expected_result) {
  const auto num_iterations = CONFIG.num_iterations;

  std::cout << "  Running " << name << " (" << num_iterations << " iterations)... " << std::flush;

  auto durations = std::vector<int64_t>{};
  durations.reserve(num_iterations);
  std::shared_ptr<Table> result = nullptr;

  for (uint32_t i = 0; i < num_iterations; ++i) {
    const auto start = std::chrono::high_resolution_clock::now();
    result = func(input);
    Hyrise::get().scheduler()->wait_for_all_tasks();
    const auto end = std::chrono::high_resolution_clock::now();

    const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    durations.push_back(duration);

    if (i == 0 && expected_result && result) {
      if (!verify_tables_equal_hc(*result, *expected_result)) {
        std::cout << "FAILED VALIDATION!" << std::endl;
        return result;
      } else {
        std::cout << "[VALIDATED] " << std::flush;
      }
    }
  }

  // Calculate statistics
  const auto min = *std::min_element(durations.begin(), durations.end());
  const auto max = *std::max_element(durations.begin(), durations.end());

  double avg;
  if (num_iterations >= 3) {
    auto sorted_durations = durations;
    std::sort(sorted_durations.begin(), sorted_durations.end());

    int64_t sum = 0;
    for (size_t i = 1; i < sorted_durations.size() - 1; ++i) {
      sum += sorted_durations[i];
    }
    avg = static_cast<double>(sum) / static_cast<double>(num_iterations - 2);
  } else {
    const auto sum = std::accumulate(durations.begin(), durations.end(), int64_t{0});
    avg = static_cast<double>(sum) / static_cast<double>(num_iterations);
  }

  std::cout << "Done." << std::endl;
  std::cout << "    Avg: " << std::fixed << std::setprecision(1) << avg << " ms";
  if (num_iterations >= 3) {
    std::cout << " (trimmed)";
  }
  std::cout << " | Min: " << min << " ms"
            << " | Max: " << max << " ms" << std::endl;

  return result;
}

void run_hash_micro_benchmark(float scale_factor) {
  std::cout << "=== Running HASH Microbenchmark ===" << std::endl;

  // 1. Data Generation
  auto input_table = generate_lineitem_data(scale_factor);
  std::shared_ptr<Table> expected_result = nullptr;

  // 2. Single Threaded Baseline (Ground Truth)
  if (CONFIG.run_single_baseline) {
    setup_scheduler(false); // Single
    std::cout << "  [Mode: Single Threaded]" << std::endl;
    expected_result = run_algorithm("Hash Single Baseline", hash_single_baseline, input_table, nullptr);
  }

  // 3. Single Threaded Optimized
  if (CONFIG.run_single_optimized) {
    setup_scheduler(false);
    std::cout << "  [Mode: Single Threaded]" << std::endl;
    run_algorithm("Hash Single Optimized", hash_single_optimized, input_table, expected_result);
  }

  // 4. Multi Baseline
  if (CONFIG.run_multi_naive) {
    setup_scheduler(true, CONFIG.num_workers);
    std::cout << "  [Mode: Multi Threaded (" << CONFIG.num_workers << " workers)]" << std::endl;
    run_algorithm("Hash Multi Naive", hash_multi_naive, input_table, expected_result);
  }

  // 5. Multi Optimized
  if (CONFIG.run_multi_optimized) {
    setup_scheduler(true, CONFIG.num_workers);
    std::cout << "  [Mode: Multi Threaded (" << CONFIG.num_workers << " workers)]" << std::endl;
    run_algorithm("Hash Multi Optimized", hash_multi_optimized, input_table, expected_result);
  }

  std::cout << "=== HASH Microbenchmark Finished ===" << std::endl;
}

void run_sort_micro_benchmark(float scale_factor) {
  std::cout << "=== Running SORT Microbenchmark ===" << std::endl;
  // 1. Data Generation
  auto input_table = generate_lineitem_data(scale_factor);
  std::shared_ptr<Table> expected_result = nullptr;

  // 2. Single Threaded Baseline (Ground Truth)
  if (CONFIG.run_single_baseline) {
    setup_scheduler(false); // Single
    std::cout << "  [Mode: Single Threaded]" << std::endl;
    expected_result = run_algorithm("Sort Single Baseline", sort_single_baseline, input_table, nullptr);
  }

  // 3. Single Threaded Optimized
  if (CONFIG.run_single_optimized) {
    setup_scheduler(false);
    std::cout << "  [Mode: Single Threaded]" << std::endl;
    run_algorithm("Sort Single Optimized", sort_single_optimized, input_table, expected_result);
  }

  // 4. Multi Baseline
  if (CONFIG.run_multi_naive) {
    setup_scheduler(true, CONFIG.num_workers);
    std::cout << "  [Mode: Multi Threaded (" << CONFIG.num_workers << " workers)]" << std::endl;
    run_algorithm("Sort Multi Naive", sort_multi_naive, input_table, expected_result);
  }

  // 5. Multi Optimized
  if (CONFIG.run_multi_optimized) {
    setup_scheduler(true, CONFIG.num_workers);
    std::cout << "  [Mode: Multi Threaded (" << CONFIG.num_workers << " workers)]" << std::endl;
    run_algorithm("Sort Multi Optimized", sort_multi_optimized, input_table, expected_result);
  }

  std::cout << "=== SORT Microbenchmark Finished ===" << std::endl;
}

void run_sort_micro_benchmark_hc(float scale_factor) {
  std::cout << "=== Running SORT Microbenchmark (high cardinality) ===" << std::endl;
  // 1. Data Generation
  auto input_table = generate_lineitem_data(scale_factor);
  std::shared_ptr<Table> expected_result = nullptr;

  // 2. Single Threaded Baseline (Ground Truth)
  if (CONFIG.run_single_baseline) {
    setup_scheduler(false); // Single
    std::cout << "  [Mode: Single Threaded]" << std::endl;
    expected_result = run_algorithm_hc("Sort Single Baseline HC", sort_single_baseline_hc, input_table, nullptr);
  }

  // 3. Single Threaded Optimized
  if (CONFIG.run_single_optimized) {
    setup_scheduler(false);
    std::cout << "  [Mode: Single Threaded]" << std::endl;
    run_algorithm_hc("Sort Single Optimized HC", sort_single_optimized_hc, input_table, expected_result);
  }

  // 4. Multi Baseline
  if (CONFIG.run_multi_naive) {
    setup_scheduler(true, CONFIG.num_workers);
    std::cout << "  [Mode: Multi Threaded (" << CONFIG.num_workers << " workers)]" << std::endl;
    run_algorithm_hc("Sort Multi Naive HC", sort_multi_naive_hc, input_table, expected_result);
  }

  // 5. Multi Optimized
  if (CONFIG.run_multi_optimized) {
    setup_scheduler(true, CONFIG.num_workers);
    std::cout << "  [Mode: Multi Threaded (" << CONFIG.num_workers << " workers)]" << std::endl;
    run_algorithm_hc("Sort Multi Optimized HC", sort_multi_optimized_hc, input_table, expected_result);
  }

  std::cout << "=== SORT Microbenchmark (high cardinality) Finished ===" << std::endl;
}


/**
 * MAIN
 */
int main() {
  std::cout << "Hyrise Playground: Hash vs Sort Aggregation" << std::endl;
  std::cout << "=============================================" << std::endl;

  // Scale factors to benchmark
  const auto scale_factors = std::vector<float>{1.0f, 2.0f, 4.0f, 8.0f, 16.0f, 32.0f};

  // Start total benchmark timer
  const auto total_start = std::chrono::high_resolution_clock::now();

  for (const auto scale_factor : scale_factors) {
    std::cout << "\n";
    std::cout << "######################################" << std::endl;
    std::cout << "# SCALE FACTOR: " << scale_factor << std::endl;
    std::cout << "# Workers: " << CONFIG.num_workers << std::endl;
    std::cout << "######################################" << std::endl;
    std::cout << "\n";

    // Start timer for this scale factor
    const auto sf_start = std::chrono::high_resolution_clock::now();


    /*************************************
     ***** Comment / Uncomment here ******
     *************************************/
    run_hash_micro_benchmark(scale_factor);
    run_sort_micro_benchmark(scale_factor);
    run_sort_micro_benchmark_hc(scale_factor);

    // Calculate and display time for this scale factor
    const auto sf_end = std::chrono::high_resolution_clock::now();
    const auto sf_duration = std::chrono::duration_cast<std::chrono::milliseconds>(sf_end - sf_start).count();

    std::cout << "\n";
    std::cout << ">>> Scale Factor " << scale_factor << " completed in "
              << sf_duration << " ms ("
              << std::fixed << std::setprecision(2) << (static_cast<double>(sf_duration) / 1000.0) << " s)" << std::endl;
  }

  // Calculate and display total benchmark time
  const auto total_end = std::chrono::high_resolution_clock::now();
  const auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_start).count();

  std::cout << "\n";
  std::cout << "=============================================" << std::endl;
  std::cout << "TOTAL BENCHMARK TIME: " << total_duration << " ms ("
            << std::fixed << std::setprecision(2) << (static_cast<double>(total_duration) / 1000.0) << " s)" << std::endl;
  std::cout << "=============================================" << std::endl;

  // Cleanup scheduler before exit
  Hyrise::get().scheduler()->finish();

  return 0;
}