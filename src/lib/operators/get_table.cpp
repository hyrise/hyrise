#include "get_table.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "all_type_variant.hpp"
#include "concurrency/transaction_context.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/pqp_utils.hpp"
#include "operators/table_scan.hpp"
#include "storage/chunk.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/pruning_utils.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

GetTable::GetTable(const std::string& name) : GetTable{name, {}, {}} {}

GetTable::GetTable(const std::string& name, const std::vector<ChunkID>& pruned_chunk_ids,
                   const std::vector<ColumnID>& pruned_column_ids)
    : AbstractReadOnlyOperator{OperatorType::GetTable},
      _name{name},
      _pruned_chunk_ids{pruned_chunk_ids},
      _pruned_column_ids{pruned_column_ids} {
  // Check pruned_chunk_ids
  DebugAssert(std::is_sorted(_pruned_chunk_ids.begin(), _pruned_chunk_ids.end()), "Expected sorted vector of ChunkIDs");
  DebugAssert(std::adjacent_find(_pruned_chunk_ids.begin(), _pruned_chunk_ids.end()) == _pruned_chunk_ids.end(),
              "Expected vector of unique ChunkIDs");

  // Check pruned_column_ids
  DebugAssert(std::is_sorted(_pruned_column_ids.begin(), _pruned_column_ids.end()),
              "Expected sorted vector of ColumnIDs");
  DebugAssert(std::adjacent_find(_pruned_column_ids.begin(), _pruned_column_ids.end()) == _pruned_column_ids.end(),
              "Expected vector of unique ColumnIDs");
}

const std::string& GetTable::name() const {
  static const auto name = std::string{"GetTable"};
  return name;
}

std::string GetTable::description(DescriptionMode description_mode) const {
  const auto stored_table = Hyrise::get().storage_manager.get_table(_name);
  const auto separator = (description_mode == DescriptionMode::SingleLine ? ' ' : '\n');
  auto stream = std::stringstream{};

  stream << AbstractOperator::description(description_mode) << separator;
  stream << "(" << table_name() << ")" << separator;
  stream << "pruned:" << separator;
  auto overall_pruned_chunk_ids = _dynamically_pruned_chunk_ids;
  overall_pruned_chunk_ids.insert(_pruned_chunk_ids.begin(), _pruned_chunk_ids.end());
  const auto overall_pruned_chunk_count = overall_pruned_chunk_ids.size();
  const auto dynamically_pruned_chunk_count = overall_pruned_chunk_count - _pruned_chunk_ids.size();

  stream << overall_pruned_chunk_count << "/" << stored_table->chunk_count() << " chunk(s)";
  if (overall_pruned_chunk_count > 0) {
    stream << " (" << _pruned_chunk_ids.size() << " static, " << dynamically_pruned_chunk_count << " dynamic)";
  }

  if (description_mode == DescriptionMode::SingleLine) {
    stream << ",";
  }
  stream << separator;
  stream << _pruned_column_ids.size() << "/" << stored_table->column_count() << " column(s)";

  return stream.str();
}

const std::string& GetTable::table_name() const {
  return _name;
}

const std::vector<ChunkID>& GetTable::pruned_chunk_ids() const {
  return _pruned_chunk_ids;
}

const std::vector<ColumnID>& GetTable::pruned_column_ids() const {
  return _pruned_column_ids;
}

void GetTable::set_prunable_subquery_predicates(
    const std::vector<std::weak_ptr<const AbstractOperator>>& subquery_scans) const {
  DebugAssert(std::all_of(subquery_scans.cbegin(), subquery_scans.cend(),
                          [](const auto& op) {
                            return op.lock() && op.lock()->type() == OperatorType::TableScan;
                          }),
              "No TableScan set as prunable predicate.");

  _prunable_subquery_scans = subquery_scans;
}

std::vector<std::shared_ptr<const AbstractOperator>> GetTable::prunable_subquery_predicates() const {
  auto subquery_scans = std::vector<std::shared_ptr<const AbstractOperator>>{};
  subquery_scans.reserve(_prunable_subquery_scans.size());
  for (const auto& subquery_scan_ref : _prunable_subquery_scans) {
    const auto& subquery_scan = subquery_scan_ref.lock();
    Assert(subquery_scan, "Referenced TableScan expired. PQP is invalid.");
    subquery_scans.emplace_back(subquery_scan);
  }
  return subquery_scans;
}

std::shared_ptr<AbstractOperator> GetTable::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& /*copied_left_input*/,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  // We cannot copy _prunable_subquery_scans here since deep_copy() recurses into the input operators and the GetTable
  // operators are the first ones to be copied. Instead, AbstractOperator::deep_copy() sets the copied TableScans after
  // the whole PQP has been copied.
  return std::make_shared<GetTable>(_name, _pruned_chunk_ids, _pruned_column_ids);
}

void GetTable::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> GetTable::_on_execute() {
  const auto stored_table = Hyrise::get().storage_manager.get_table(_name);

  // The chunk count might change while we are in this method as other threads concurrently insert new data. MVCC
  // guarantees that rows that are inserted after this transaction was started (and thus after GetTable started to
  // execute) are not visible. Thus, we do not have to care about chunks added after this point. By retrieving
  // chunk_count only once, we avoid concurrency issues, for example when more chunks are added to output_chunks than
  // entries were originally allocated.
  const auto chunk_count = stored_table->chunk_count();

  /**
   * Build a sorted vector (`excluded_chunk_ids`) of physically/logically deleted and pruned ChunkIDs
   */
  DebugAssert(!transaction_context_is_set() || transaction_context()->phase() == TransactionPhase::Active,
              "Transaction is not active anymore.");
  if (HYRISE_DEBUG && !transaction_context_is_set()) {
    for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
      DebugAssert(stored_table->get_chunk(chunk_id) && !stored_table->get_chunk(chunk_id)->get_cleanup_commit_id(),
                  "For TableType::Data tables with deleted chunks, the transaction context must be set.");
    }
  }

  // Currently, value_clustered_by is only used for temporary tables. If tables in the StorageManager start using that
  // flag, too, it needs to be forwarded here; otherwise it would be completely invisible in the PQP.
  DebugAssert(stored_table->value_clustered_by().empty(), "GetTable does not forward value_clustered_by");
  auto overall_pruned_chunk_ids = _prune_chunks_dynamically();
  overall_pruned_chunk_ids.insert(_pruned_chunk_ids.cbegin(), _pruned_chunk_ids.cend());
  auto pruned_chunk_ids_iter = overall_pruned_chunk_ids.begin();
  auto excluded_chunk_ids = std::vector<ChunkID>{};
  for (auto stored_chunk_id = ChunkID{0}; stored_chunk_id < chunk_count; ++stored_chunk_id) {
    // Check whether the Chunk is pruned
    if (pruned_chunk_ids_iter != overall_pruned_chunk_ids.end() && *pruned_chunk_ids_iter == stored_chunk_id) {
      ++pruned_chunk_ids_iter;
      excluded_chunk_ids.emplace_back(stored_chunk_id);
      continue;
    }

    const auto chunk = stored_table->get_chunk(stored_chunk_id);

    // Skip chunks that were physically deleted
    if (!chunk) {
      excluded_chunk_ids.emplace_back(stored_chunk_id);
      continue;
    }

    // Skip chunks that were just inserted by a different transaction and that do not have any content yet
    if (chunk->size() == 0) {
      excluded_chunk_ids.emplace_back(stored_chunk_id);
      continue;
    }

    // Check whether the Chunk is logically deleted
    if (transaction_context_is_set() && chunk->get_cleanup_commit_id() &&
        *chunk->get_cleanup_commit_id() <= transaction_context()->snapshot_commit_id()) {
      excluded_chunk_ids.emplace_back(stored_chunk_id);
      continue;
    }
  }

  // We cannot create a Table without columns - since Chunks rely on their first column to determine their row count
  Assert(_pruned_column_ids.size() < static_cast<size_t>(stored_table->column_count()),
         "Cannot prune all columns from Table");
  DebugAssert(std::all_of(_pruned_column_ids.begin(), _pruned_column_ids.end(),
                          [&](const auto column_id) {
                            return column_id < stored_table->column_count();
                          }),
              "ColumnID out of range");

  /**
   * Build pruned TableColumnDefinitions of the output Table
   */
  auto pruned_column_definitions = TableColumnDefinitions{};
  if (_pruned_column_ids.empty()) {
    pruned_column_definitions = stored_table->column_definitions();
  } else {
    pruned_column_definitions =
        TableColumnDefinitions{stored_table->column_definitions().size() - _pruned_column_ids.size()};

    auto pruned_column_ids_iter = _pruned_column_ids.begin();
    for (auto stored_column_id = ColumnID{0}, output_column_id = ColumnID{0};
         stored_column_id < stored_table->column_count(); ++stored_column_id) {
      if (pruned_column_ids_iter != _pruned_column_ids.end() && stored_column_id == *pruned_column_ids_iter) {
        ++pruned_column_ids_iter;
        continue;
      }

      pruned_column_definitions[output_column_id] = stored_table->column_definitions()[stored_column_id];
      ++output_column_id;
    }
  }

  /**
   * Build the output Table, omitting pruned Chunks and Columns as well as deleted Chunks
   */
  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{chunk_count - excluded_chunk_ids.size()};
  auto output_chunks_iter = output_chunks.begin();

  auto excluded_chunk_ids_iter = excluded_chunk_ids.begin();

  for (auto stored_chunk_id = ChunkID{0}; stored_chunk_id < chunk_count; ++stored_chunk_id) {
    // Skip `stored_chunk_id` if it is in the sorted vector `excluded_chunk_ids`
    if (excluded_chunk_ids_iter != excluded_chunk_ids.end() && *excluded_chunk_ids_iter == stored_chunk_id) {
      ++excluded_chunk_ids_iter;
      continue;
    }

    // The Chunk is to be included in the output Table, now we progress to excluding Columns
    const auto stored_chunk = stored_table->get_chunk(stored_chunk_id);

    // Make a copy of the order-by information of the current chunk. This information is adapted when columns are
    // pruned and will be set on the output chunk.
    const auto& input_chunk_sorted_by = stored_chunk->individually_sorted_by();
    auto chunk_sort_definition = std::optional<SortColumnDefinition>{};

    if (_pruned_column_ids.empty()) {
      *output_chunks_iter = stored_chunk;
    } else {
      const auto column_count = stored_table->column_count();
      auto output_segments = Segments{column_count - _pruned_column_ids.size()};
      auto output_segments_iter = output_segments.begin();
      auto output_indexes = Indexes{};

      auto pruned_column_ids_iter = _pruned_column_ids.begin();
      for (auto stored_column_id = ColumnID{0}; stored_column_id < column_count; ++stored_column_id) {
        // Skip `stored_column_id` if it is in the sorted vector `_pruned_column_ids`
        if (pruned_column_ids_iter != _pruned_column_ids.end() && stored_column_id == *pruned_column_ids_iter) {
          ++pruned_column_ids_iter;
          continue;
        }

        if (!input_chunk_sorted_by.empty()) {
          for (const auto& sorted_by : input_chunk_sorted_by) {
            if (sorted_by.column == stored_column_id) {
              const auto columns_pruned_so_far = std::distance(_pruned_column_ids.begin(), pruned_column_ids_iter);
              const auto new_sort_column =
                  ColumnID{static_cast<uint16_t>(static_cast<size_t>(stored_column_id) - columns_pruned_so_far)};
              chunk_sort_definition = SortColumnDefinition(new_sort_column, sorted_by.sort_mode);
            }
          }
        }

        *output_segments_iter = stored_chunk->get_segment(stored_column_id);
        auto indexes = stored_chunk->get_indexes({*output_segments_iter});
        if (!indexes.empty()) {
          output_indexes.insert(std::end(output_indexes), std::begin(indexes), std::end(indexes));
        }
        ++output_segments_iter;
      }

      *output_chunks_iter = std::make_shared<Chunk>(std::move(output_segments), stored_chunk->mvcc_data(),
                                                    stored_chunk->get_allocator(), std::move(output_indexes));

      if (!stored_chunk->is_mutable()) {
        // Marking the chunk as immutable is cheap here: the MvccData's `max_begin_cid` is already set, so
        // `set_immutable()` only sets the flag and does not trigger anything else.
        (*output_chunks_iter)->set_immutable();
      }

      if (chunk_sort_definition) {
        (*output_chunks_iter)->set_individually_sorted_by(*chunk_sort_definition);
      }

      // The output chunk contains all rows that are in the stored chunk, including invalid rows. We forward this
      // information so that following operators (currently, the Validate operator) can use it for optimizations.
      // Incrementing atomic invalid row count in relaxed memory order as chunk is not yet visible.
      (*output_chunks_iter)->increase_invalid_row_count(stored_chunk->invalid_row_count(), std::memory_order_relaxed);
    }

    ++output_chunks_iter;
  }

  // Lambda to check if all chunks indexed by a table index have been pruned by the ChunkPruningRule or
  // ColumnPruningRule of the optimizer.
  const auto all_indexed_segments_pruned = [&](const auto& table_index) {
    // Check if indexed ColumnID has been pruned.
    const auto indexed_column_id = table_index->get_indexed_column_id();
    if (std::find(_pruned_column_ids.cbegin(), _pruned_column_ids.cend(), indexed_column_id) !=
        _pruned_column_ids.cend()) {
      return true;
    }

    const auto indexed_chunk_ids = table_index->get_indexed_chunk_ids();

    // Early out if index is empty.
    if (indexed_chunk_ids.empty()) {
      return false;
    }

    // Check if the indexed chunks have been pruned.
    DebugAssert(std::is_sorted(_pruned_chunk_ids.begin(), _pruned_chunk_ids.end()),
                "Expected _pruned_chunk_ids vector to be sorted.");
    return std::all_of(indexed_chunk_ids.cbegin(), indexed_chunk_ids.cend(), [&](const auto chunk_id) {
      return std::binary_search(_pruned_chunk_ids.cbegin(), _pruned_chunk_ids.cend(), chunk_id);
    });
  };

  auto table_indexes = stored_table->get_table_indexes();
  table_indexes.erase(std::remove_if(table_indexes.begin(), table_indexes.end(), all_indexed_segments_pruned),
                      table_indexes.cend());

  return std::make_shared<Table>(pruned_column_definitions, TableType::Data, std::move(output_chunks),
                                 stored_table->uses_mvcc(), table_indexes);
}

std::set<ChunkID> GetTable::_prune_chunks_dynamically() {
  if (_prunable_subquery_scans.empty()) {
    return {};
  }

  // Create a dummy PredicateNode for each predicate containing a subquery that has already been executed. We do not use
  // the original predicate to ignore all other nodes between the StoredTableNode and the PredicateNodes. Since the
  // ChunkPruningRule already took care to add only predicates that are safe to prune with, we can act as if there were
  // no other LQP nodes.
  auto prunable_predicate_nodes = std::vector<std::shared_ptr<PredicateNode>>{};
  prunable_predicate_nodes.reserve(_prunable_subquery_scans.size());

  // Create a dummy StoredTableNode from the table to retrieve. `compute_chunk_exclude_list` modifies the node's
  // statistics and we want to avoid that. We cannot use `deep_copy()` here since it would complain that the referenced
  // prunable PredicateNodes are not part of the LQP.
  const auto& stored_table_node = static_cast<const StoredTableNode&>(*lqp_node);
  const auto dummy_stored_table_node = StoredTableNode::make(_name);

  for (const auto& op : prunable_subquery_predicates()) {
    const auto& table_scan = static_cast<const TableScan&>(*op);
    const auto& operator_predicate_arguments = table_scan.predicate()->arguments;
    const auto& predicate_node = static_cast<const PredicateNode&>(*table_scan.lqp_node);
    const auto adjusted_predicate = predicate_node.predicate()->deep_copy();
    auto& arguments = adjusted_predicate->arguments;
    const auto argument_count = adjusted_predicate->arguments.size();

    // Adjust predicates with the dummy StoredTableNode and the subquery result, if available.
    for (auto expression_idx = size_t{0}; expression_idx < argument_count; ++expression_idx) {
      auto& argument = arguments[expression_idx];
      // Replace any column with the respective column from our dummy StoredTableNode.
      if (const auto lqp_column = std::dynamic_pointer_cast<LQPColumnExpression>(argument)) {
        Assert(*lqp_column->original_node.lock() == stored_table_node,
               "Predicate is performed on wrong StoredTableNode.");
        argument = lqp_column_(dummy_stored_table_node, lqp_column->original_column_id);
        continue;
      }

      // Check if expression is an uncorrelated subquery.
      if (argument->type != ExpressionType::LQPSubquery) {
        continue;
      }
      Assert(operator_predicate_arguments[expression_idx]->type == ExpressionType::PQPSubquery,
             "Cannot resolve PQPSubqueryExpression.");
      const auto& subquery = static_cast<PQPSubqueryExpression&>(*operator_predicate_arguments[expression_idx]);
      if (subquery.is_correlated()) {
        continue;
      }

      // It might happen that scheduling the subquery before the GetTable operator would create a cycle. For instance,
      // this can happen for a query like this: SELECT * FROM a_table WHERE x > (SELECT AVG(x) FROM a_table);
      // The PQP of the query could look like the following:
      //
      //     [TableScan] x > SUBQUERY
      //          |             *
      //          |             * uncorrelated subquery
      //          |             *
      //          |      [AggregateHash] AVG(x)
      //          |       /
      //         [GetTable] a_table
      //
      // We cannot schedule the AggregateHash operator before the GetTable operator to obtain the subquery result for
      // pruning: the OperatorTasks wrapping both operators would be in a circular wait for each other. We simply avoid
      // this circular wait by StoredTableNodes using their prunable_subquery_predicates for equality checks. Thus, the
      // LQPTranslator creates two GetTable operators rather than deduplicating them. resolve_uncorrelated_subquery()
      // asserts that the subquery has already been executed.
      argument = value_(resolve_uncorrelated_subquery(subquery.pqp));
    }

    // Add a new PredicateNode to the pruning chain.
    auto input_node = static_pointer_cast<AbstractLQPNode>(dummy_stored_table_node);
    if (!prunable_predicate_nodes.empty()) {
      input_node = prunable_predicate_nodes.back();
    }
    prunable_predicate_nodes.emplace_back(PredicateNode::make(adjusted_predicate, input_node));
  }

  _dynamically_pruned_chunk_ids = compute_chunk_exclude_list(prunable_predicate_nodes, dummy_stored_table_node);
  return _dynamically_pruned_chunk_ids;
}

}  // namespace hyrise
