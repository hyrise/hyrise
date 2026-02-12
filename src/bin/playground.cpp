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
  bool run_multi_optimized =true;
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

std::shared_ptr<hyrise::Table> sort_single_optimized(const std::shared_ptr<hyrise::Table>& input) {
  using namespace hyrise;

  const auto rf_col = input->column_id_by_name("l_returnflag");
  const auto ls_col = input->column_id_by_name("l_linestatus");
  const auto qty_col = input->column_id_by_name("l_quantity");

  // 1️⃣ OPTIMIZED STRUCT WITH PACKED DATA
  struct alignas(16) RowData {
    uint16_t rf_id;
    uint16_t ls_id;
    float qty;
    uint32_t key() const { return (static_cast<uint32_t>(rf_id) << 16) | ls_id; }
  };
  
  const size_t total_rows = input->row_count();
  std::vector<RowData> rows;
  rows.reserve(total_rows);
  
  // 2️⃣ FAST ENCODING WITH DIRECT CHARACTER MAPPING
  std::array<uint8_t, 256> char_to_id = {0};
  char_to_id['R'] = 0; char_to_id['A'] = 1; char_to_id['N'] = 2;
  char_to_id['F'] = 0; char_to_id['O'] = 1; char_to_id['P'] = 2;
  
  std::array<char, 3> rf_id_to_char = {'R', 'A', 'N'};
  std::array<char, 3> ls_id_to_char = {'F', 'O', 'P'};
  
  // 3️⃣ BULK EXTRACTION WITH UNROLLED LOOP
  for (ChunkID chunk_id{0}; chunk_id < input->chunk_count(); ++chunk_id) {
    const auto chunk = input->get_chunk(chunk_id);
    if (!chunk || chunk->size() == 0) continue;
    
    const auto& rf_seg = chunk->get_segment(rf_col);
    const auto& ls_seg = chunk->get_segment(ls_col);
    const auto& qty_seg = chunk->get_segment(qty_col);
    
    const auto chunk_size = chunk->size();
    const size_t unroll_factor = 8;  // Increased unroll factor for single-threaded
    
    // Process 8 rows at a time for better instruction-level parallelism
    for (size_t offset = 0; offset + unroll_factor <= chunk_size; offset += unroll_factor) {
      RowData rows_data[8];
      
      // Unrolled extraction
      for (size_t i = 0; i < unroll_factor; ++i) {
        ChunkOffset co = static_cast<ChunkOffset>(offset + i);
        const pmr_string rf_val = boost::get<pmr_string>((*rf_seg)[co]);
        const pmr_string ls_val = boost::get<pmr_string>((*ls_seg)[co]);
        
        rows_data[i].rf_id = char_to_id[static_cast<uint8_t>(rf_val[0])];
        rows_data[i].ls_id = char_to_id[static_cast<uint8_t>(ls_val[0])];
        rows_data[i].qty = boost::get<float>((*qty_seg)[co]);
      }
      
      // Batch push back
      for (size_t i = 0; i < unroll_factor; ++i) {
        rows.push_back(rows_data[i]);
      }
    }
    
    // Process remaining rows
    for (size_t offset = chunk_size - (chunk_size % unroll_factor); offset < chunk_size; ++offset) {
      ChunkOffset co = static_cast<ChunkOffset>(offset);
      const pmr_string rf_val = boost::get<pmr_string>((*rf_seg)[co]);
      const pmr_string ls_val = boost::get<pmr_string>((*ls_seg)[co]);
      
      rows.push_back({
        char_to_id[static_cast<uint8_t>(rf_val[0])],
        char_to_id[static_cast<uint8_t>(ls_val[0])],
        boost::get<float>((*qty_seg)[co])
      });
    }
  }
  
  // 4️⃣ SINGLE-THREADED RADIX SORT (OPTIMIZED FOR SMALL KEY SPACE)
  auto optimized_radix_sort = [](std::vector<RowData>& data) {
    if (data.empty()) return;
    
    const size_t n = data.size();
    
    // For small datasets, pdqsort is faster than radix sort
    if (n < 2000) {
      boost::sort::pdqsort(data.begin(), data.end(), 
                [](const RowData& a, const RowData& b) { return a.key() < b.key(); });
      return;
    }
    
    // Optimized in-place radix sort for 32-bit keys
    std::vector<RowData> buffer(n);
    
    // Pass 1: Lower 16 bits (ls_id)
    {
      std::array<uint32_t, 65536> count = {0};  // 2^16 buckets
      
      // Count frequencies
      for (const auto& row : data) {
        count[row.ls_id]++;
      }
      
      // Prefix sum
      uint32_t sum = 0;
      for (size_t i = 0; i < 65536; ++i) {
        uint32_t tmp = count[i];
        count[i] = sum;
        sum += tmp;
      }
      
      // Distribute
      for (const auto& row : data) {
        buffer[count[row.ls_id]++] = row;
      }
      
      data.swap(buffer);
    }
    
    // Pass 2: Higher 16 bits (rf_id)
    {
      std::array<uint32_t, 65536> count = {0};
      
      for (const auto& row : data) {
        count[row.rf_id]++;
      }
      
      uint32_t sum = 0;
      for (size_t i = 0; i < 65536; ++i) {
        uint32_t tmp = count[i];
        count[i] = sum;
        sum += tmp;
      }
      
      for (const auto& row : data) {
        buffer[count[row.rf_id]++] = row;
      }
      
      data.swap(buffer);
    }
  };
  
  // Apply optimized radix sort
  optimized_radix_sort(rows);
  
  // 5️⃣ SINGLE-THREADED SIMD-ACCELERATED AGGREGATION
  struct alignas(32) AggResult {
    uint32_t key;
    double sum;
  };
  
  std::vector<AggResult> aggregates;
  aggregates.reserve(9);  // Max 3x3 combinations
  
  if (!rows.empty()) {
    size_t i = 0;
    const size_t n = rows.size();
    
    // Prefetch first cache line
    __builtin_prefetch(&rows[0], 0, 3);
    
    #ifdef __AVX2__
    // SIMD-accelerated aggregation with prefetching
    while (i < n) {
      uint32_t current_key = rows[i].key();
      __m256d sum_vec = _mm256_setzero_pd();
      
      // Prefetch next group
      if (i + 64 < n) {
        __builtin_prefetch(&rows[i + 64], 0, 3);
      }
      
      // Process in SIMD batches of 4
      size_t simd_count = 0;
      while (i + 3 < n && rows[i].key() == current_key && 
             rows[i+1].key() == current_key && 
             rows[i+2].key() == current_key && 
             rows[i+3].key() == current_key) {
        
        // Load 4 floats and convert to doubles
        __m128 float_vec = _mm_set_ps(rows[i+3].qty, rows[i+2].qty, 
                                       rows[i+1].qty, rows[i].qty);
        __m256d double_vec = _mm256_cvtps_pd(float_vec);
        sum_vec = _mm256_add_pd(sum_vec, double_vec);
        
        i += 4;
        simd_count += 4;
      }
      
      // Horizontal sum
      double simd_sum = 0.0;
      if (simd_count > 0) {
        alignas(32) double temp[4];
        _mm256_store_pd(temp, sum_vec);
        simd_sum = temp[0] + temp[1] + temp[2] + temp[3];
      }
      
      // Process remaining rows with scalar code
      double scalar_sum = 0.0;
      while (i < n && rows[i].key() == current_key) {
        scalar_sum += rows[i].qty;
        i++;
      }
      
      aggregates.push_back({current_key, simd_sum + scalar_sum});
    }
    #else
    // Non-SIMD fallback with manual prefetching
    uint32_t current_key = rows[0].key();
    double current_sum = static_cast<double>(rows[0].qty);
    
    for (size_t idx = 1; idx < n; ++idx) {
      // Prefetch ahead
      if (idx + 16 < n) {
        __builtin_prefetch(&rows[idx + 16], 0, 0);
      }
      
      if (rows[idx].key() == current_key) {
        current_sum += rows[idx].qty;
      } else {
        aggregates.push_back({current_key, current_sum});
        current_key = rows[idx].key();
        current_sum = rows[idx].qty;
      }
    }
    aggregates.push_back({current_key, current_sum});
    #endif
  }
  
  // 6️⃣ OPTIMIZED RESULT BUILDING WITH BATCH APPEND
  TableColumnDefinitions columns{
      {"l_returnflag", DataType::String, false},
      {"l_linestatus", DataType::String, false},
      {"sum_qty", DataType::Double, false}
  };
  
  auto result = std::make_shared<Table>(columns, TableType::Data);
  
  // Pre-allocate batch vectors
  std::vector<pmr_string> rf_strings;
  std::vector<pmr_string> ls_strings;
  std::vector<double> sums;
  rf_strings.reserve(aggregates.size());
  ls_strings.reserve(aggregates.size());
  sums.reserve(aggregates.size());
  
  // Build strings in batch
  for (const auto& agg : aggregates) {
    uint16_t rf_id = (agg.key >> 16) & 0xFFFF;
    uint16_t ls_id = agg.key & 0xFFFF;
    
    rf_strings.emplace_back(1, rf_id < 3 ? rf_id_to_char[rf_id] : '?');
    ls_strings.emplace_back(1, ls_id < 3 ? ls_id_to_char[ls_id] : '?');
    sums.push_back(agg.sum);
  }
  
  // Batch append using the table's append_multiple if available
  for (size_t i = 0; i < aggregates.size(); ++i) {
    result->append({
      AllTypeVariant(rf_strings[i]),
      AllTypeVariant(ls_strings[i]),
      AllTypeVariant(sums[i])
    });
  }
  
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

std::shared_ptr<hyrise::Table> sort_multi_optimized(const std::shared_ptr<hyrise::Table>& input) {
  using namespace hyrise;

  const auto rf_col = input->column_id_by_name("l_returnflag");
  const auto ls_col = input->column_id_by_name("l_linestatus");
  const auto qty_col = input->column_id_by_name("l_quantity");

  // 1️⃣ OPTIMIZED STRUCT WITH PACKED DATA
  struct alignas(16) RowData {  // 16-byte alignment for SIMD
    uint16_t rf_id;    // 2 bytes
    uint16_t ls_id;    // 2 bytes  
    float qty;         // 4 bytes
    uint32_t key() const { return (static_cast<uint32_t>(rf_id) << 16) | ls_id; }
  };
  
  const size_t total_rows = input->row_count();
  std::vector<RowData> rows;
  rows.reserve(total_rows);
  
  // 2️⃣ FAST ENCODING WITH DIRECT CHARACTER MAPPING
  // TPCH has known values: R=0, A=1, N=2, F=0, O=1, P=2
  std::array<uint8_t, 256> char_to_id = {0};
  char_to_id['R'] = 0; char_to_id['A'] = 1; char_to_id['N'] = 2;
  char_to_id['F'] = 0; char_to_id['O'] = 1; char_to_id['P'] = 2;
  
  std::array<char, 3> rf_id_to_char = {'R', 'A', 'N'};
  std::array<char, 3> ls_id_to_char = {'F', 'O', 'P'};
  
  // 3️⃣ BULK EXTRACTION WITH UNROLLED LOOP - FIXED
  for (ChunkID chunk_id{0}; chunk_id < input->chunk_count(); ++chunk_id) {
    const auto chunk = input->get_chunk(chunk_id);
    if (!chunk || chunk->size() == 0) continue;
    
    const auto& rf_seg = chunk->get_segment(rf_col);
    const auto& ls_seg = chunk->get_segment(ls_col);
    const auto& qty_seg = chunk->get_segment(qty_col);
    
    const auto chunk_size = chunk->size();
    const size_t unroll_factor = 4;  // Use size_t instead of ChunkOffset
    
    // Process 4 rows at a time (loop unrolling)
    for (size_t offset = 0; offset + unroll_factor <= chunk_size; offset += unroll_factor) {
      RowData row0, row1, row2, row3;
      
      // Convert size_t to ChunkOffset for segment access
      ChunkOffset co0 = static_cast<ChunkOffset>(offset);
      ChunkOffset co1 = static_cast<ChunkOffset>(offset + 1);
      ChunkOffset co2 = static_cast<ChunkOffset>(offset + 2);
      ChunkOffset co3 = static_cast<ChunkOffset>(offset + 3);
      
      // Row 0
      row0.rf_id = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co0])[0])];
      row0.ls_id = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co0])[0])];
      row0.qty = boost::get<float>((*qty_seg)[co0]);
      
      // Row 1
      row1.rf_id = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co1])[0])];
      row1.ls_id = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co1])[0])];
      row1.qty = boost::get<float>((*qty_seg)[co1]);
      
      // Row 2
      row2.rf_id = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co2])[0])];
      row2.ls_id = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co2])[0])];
      row2.qty = boost::get<float>((*qty_seg)[co2]);
      
      // Row 3
      row3.rf_id = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*rf_seg)[co3])[0])];
      row3.ls_id = char_to_id[static_cast<uint8_t>(boost::get<pmr_string>((*ls_seg)[co3])[0])];
      row3.qty = boost::get<float>((*qty_seg)[co3]);
      
      rows.push_back(row0);
      rows.push_back(row1);
      rows.push_back(row2);
      rows.push_back(row3);
    }
    
    // Process remaining rows
    for (size_t offset_remaining = chunk_size - (chunk_size % unroll_factor); 
         offset_remaining < chunk_size; ++offset_remaining) {
      ChunkOffset co = static_cast<ChunkOffset>(offset_remaining);
      RowData row;
      
      // Safe string access
      pmr_string rf_val = boost::get<pmr_string>((*rf_seg)[co]);
      pmr_string ls_val = boost::get<pmr_string>((*ls_seg)[co]);
      
      row.rf_id = rf_val.empty() ? 0 : char_to_id[static_cast<uint8_t>(rf_val[0])];
      row.ls_id = ls_val.empty() ? 0 : char_to_id[static_cast<uint8_t>(ls_val[0])];
      row.qty = boost::get<float>((*qty_seg)[co]);
      
      rows.push_back(row);
    }
  }
  
  // 4️⃣ PARALLEL RADIX SORT (FASTER THAN COMPARISON SORT FOR INTEGERS)
  auto parallel_radix_sort = [](std::vector<RowData>& data) {
    if (data.size() < 10000) {
      std::sort(data.begin(), data.end(), 
                [](const RowData& a, const RowData& b) { return a.key() < b.key(); });
      return;
    }
    
    // Use counting sort/radix sort for 32-bit keys
    constexpr size_t RADIX_BITS = 8;
    constexpr size_t RADIX_SIZE = 1 << RADIX_BITS;
    constexpr size_t PASSES = 4;  // 32 bits / 8 bits
    
    std::vector<RowData> buffer(data.size());
    std::array<size_t, RADIX_SIZE> count;
    
    for (size_t pass = 0; pass < PASSES; ++pass) {
      // Reset counts
      std::fill(count.begin(), count.end(), 0);
      
      // Count occurrences (can be parallelized)
      const size_t shift = pass * RADIX_BITS;
      for (const auto& row : data) {
        uint8_t digit = (row.key() >> shift) & 0xFF;
        count[digit]++;
      }
      
      // Convert to positions
      size_t total = 0;
      for (size_t i = 0; i < RADIX_SIZE; ++i) {
        size_t old_count = count[i];
        count[i] = total;
        total += old_count;
      }
      
      // Distribute
      for (const auto& row : data) {
        uint8_t digit = (row.key() >> shift) & 0xFF;
        buffer[count[digit]++] = row;
      }
      
      // Swap buffers
      data.swap(buffer);
    }
  };
  
  // Sort using parallel radix sort
  parallel_radix_sort(rows);
  
  // 5️⃣ SIMD-ACCELERATED AGGREGATION
  struct alignas(32) AggResult {  // 32-byte alignment for AVX
    uint32_t key;
    double sum;
    size_t count;
  };
  
  std::vector<AggResult> aggregates;
  
  if (!rows.empty()) {
    aggregates.reserve(9);  // Max 3x3 combinations for TPCH
    
    size_t i = 0;
    const size_t n = rows.size();
    
    #ifdef __AVX2__
    // SIMD-accelerated aggregation for consecutive same keys
    while (i < n) {
      uint32_t current_key = rows[i].key();
      __m256d sum_vec = _mm256_setzero_pd();
      size_t simd_count = 0;
      
      // Process in blocks of 4 while keys are the same
      while (i + 3 < n && rows[i].key() == current_key && 
             rows[i+1].key() == current_key && 
             rows[i+2].key() == current_key && 
             rows[i+3].key() == current_key) {
        
        // Load 4 floats into SIMD registers
        __m128 float_vec = _mm_set_ps(rows[i+3].qty, rows[i+2].qty, 
                                       rows[i+1].qty, rows[i].qty);
        
        // Convert float to double
        __m256d double_vec = _mm256_cvtps_pd(float_vec);
        sum_vec = _mm256_add_pd(sum_vec, double_vec);
        
        i += 4;
        simd_count += 4;
      }
      
      // Horizontal sum of SIMD vector
      double simd_sum = 0.0;
      if (simd_count > 0) {
        alignas(32) double temp[4];
        _mm256_store_pd(temp, sum_vec);
        simd_sum = temp[0] + temp[1] + temp[2] + temp[3];
      }
      
      // Process remaining rows with same key
      double scalar_sum = 0.0;
      size_t scalar_count = 0;
      while (i < n && rows[i].key() == current_key) {
        scalar_sum += rows[i].qty;
        i++;
        scalar_count++;
      }
      
      aggregates.push_back({current_key, simd_sum + scalar_sum, 
                           simd_count + scalar_count});
    }
    #else
    // Non-SIMD fallback
    uint32_t current_key = rows[0].key();
    double current_sum = static_cast<double>(rows[0].qty);
    size_t current_count = 1;
    
    for (size_t idx = 1; idx < n; ++idx) {
      if (rows[idx].key() == current_key) {
        current_sum += rows[idx].qty;
        current_count++;
      } else {
        aggregates.push_back({current_key, current_sum, current_count});
        current_key = rows[idx].key();
        current_sum = rows[idx].qty;
        current_count = 1;
      }
    }
    aggregates.push_back({current_key, current_sum, current_count});
    #endif
  }
  
  // 6️⃣ OPTIMIZED RESULT BUILDING
  TableColumnDefinitions columns{
      {"l_returnflag", DataType::String, false},
      {"l_linestatus", DataType::String, false},
      {"sum_qty", DataType::Double, false}
  };
  
  auto result = std::make_shared<Table>(columns, TableType::Data, Chunk::DEFAULT_SIZE);
  
  // Build all rows in a batch
  std::vector<std::vector<AllTypeVariant>> batch_rows;
  batch_rows.reserve(aggregates.size());
  
  for (const auto& agg : aggregates) {
    uint16_t rf_id = (agg.key >> 16) & 0xFFFF;
    uint16_t ls_id = agg.key & 0xFFFF;
    
    // Convert IDs back to strings - ensure bounds check
    pmr_string rf_str(1, rf_id < 3 ? rf_id_to_char[rf_id] : '?');
    pmr_string ls_str(1, ls_id < 3 ? ls_id_to_char[ls_id] : '?');
    
    batch_rows.push_back({AllTypeVariant(rf_str), 
                         AllTypeVariant(ls_str), 
                         AllTypeVariant(agg.sum)});
  }
  
  // Append all rows
  for (const auto& row : batch_rows) {
    result->append(row);
  }
  
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

/**
 * BENCHMARK DRIVER
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
    Hyrise::get().scheduler()->wait_for_all_tasks();  // Ensure async tasks are done
    const auto end = std::chrono::high_resolution_clock::now();

    const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    durations.push_back(duration);

    // Verify result on first iteration only
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
    // Trimmed mean: exclude ONE min and ONE max to reduce impact of outliers
    auto sorted_durations = durations;
    std::sort(sorted_durations.begin(), sorted_durations.end());

    // Sum middle values (exclude first and last)
    int64_t sum = 0;
    for (size_t i = 1; i < sorted_durations.size() - 1; ++i) {
      sum += sorted_durations[i];
    }
    avg = static_cast<double>(sum) / static_cast<double>(num_iterations - 2);
  } else {
    // For n < 3, use all values
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