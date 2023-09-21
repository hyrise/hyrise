#include "table.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_manager.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/index/partial_hash/partial_hash_index.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

// Checks if the two vectors have common elements, e.g., lhs = [0, 2] and rhs = [2, 1, 3]. We expect very short
// vectors, so we use a simple but quadratic solution. For larger vectors, we could create sets and check set
// containment.
bool columns_intersect(const std::vector<ColumnID>& lhs, const std::vector<ColumnID>& rhs) {
  for (const auto column_id : lhs) {
    if (std::find(rhs.cbegin(), rhs.cend(), column_id) != rhs.cend()) {
      return true;
    }
  }
  return false;
}

bool columns_intersect(const std::set<ColumnID>& lhs, const std::set<ColumnID>& rhs) {
  for (const auto column_id : lhs) {
    if (rhs.contains(column_id)) {
      return true;
    }
  }
  return false;
}

}  // namespace

namespace hyrise {

std::shared_ptr<Table> Table::create_dummy_table(const TableColumnDefinitions& column_definitions) {
  return std::make_shared<Table>(column_definitions, TableType::Data);
}

Table::Table(const TableColumnDefinitions& column_definitions, const TableType type,
             const std::optional<ChunkOffset> target_chunk_size, const UseMvcc use_mvcc,
             const pmr_vector<std::shared_ptr<PartialHashIndex>>& table_indexes)
    : _column_definitions(column_definitions),
      _type(type),
      _use_mvcc(use_mvcc),
      _target_chunk_size(type == TableType::Data ? target_chunk_size.value_or(Chunk::DEFAULT_SIZE) : Chunk::MAX_SIZE),
      _append_mutex(std::make_unique<std::mutex>()),
      _table_indexes(table_indexes) {
  DebugAssert(target_chunk_size <= Chunk::MAX_SIZE, "Chunk size exceeds maximum");
  DebugAssert(type == TableType::Data || !target_chunk_size, "Must not set target_chunk_size for reference tables");
  DebugAssert(!target_chunk_size || *target_chunk_size > 0, "Table must have a chunk size greater than 0.");
}

Table::Table(const TableColumnDefinitions& column_definitions, const TableType type,
             std::vector<std::shared_ptr<Chunk>>&& chunks, const UseMvcc use_mvcc,
             pmr_vector<std::shared_ptr<PartialHashIndex>> const& table_indexes)
    : Table(column_definitions, type, type == TableType::Data ? std::optional{Chunk::DEFAULT_SIZE} : std::nullopt,
            use_mvcc, table_indexes) {
  _chunks = {chunks.begin(), chunks.end()};

  if constexpr (HYRISE_DEBUG) {
    const auto chunk_count = _chunks.size();
    const auto num_columns = column_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = get_chunk(chunk_id);
      if (!chunk) {
        continue;
      }

      Assert(chunk->size() > 0 || (type == TableType::Data && chunk_id == chunk_count - 1 && chunk->is_mutable()),
             "Empty chunk other than mutable chunk at the end was found");
      Assert(chunk->has_mvcc_data() == (_use_mvcc == UseMvcc::Yes), "Supply MvccData for Chunks iff Table uses MVCC");
      Assert(chunk->column_count() == num_columns, "Invalid Chunk column count");

      for (auto column_id = ColumnID{0}; column_id < num_columns; ++column_id) {
        Assert(chunk->get_segment(column_id)->data_type() == column_data_type(column_id), "Invalid Segment DataType");
      }
    }
  }
}

const TableColumnDefinitions& Table::column_definitions() const {
  return _column_definitions;
}

TableType Table::type() const {
  return _type;
}

UseMvcc Table::uses_mvcc() const {
  return _use_mvcc;
}

ColumnCount Table::column_count() const {
  return ColumnCount{static_cast<ColumnCount::base_type>(_column_definitions.size())};
}

const std::string& Table::column_name(const ColumnID column_id) const {
  Assert(column_id < _column_definitions.size(), "ColumnID out of range.");
  return _column_definitions[column_id].name;
}

std::vector<std::string> Table::column_names() const {
  auto names = std::vector<std::string>{};
  names.reserve(_column_definitions.size());
  for (const auto& column_definition : _column_definitions) {
    names.emplace_back(column_definition.name);
  }
  return names;
}

DataType Table::column_data_type(const ColumnID column_id) const {
  Assert(column_id < _column_definitions.size(), "ColumnID out of range.");
  return _column_definitions[column_id].data_type;
}

std::vector<DataType> Table::column_data_types() const {
  auto types = std::vector<DataType>{};
  types.reserve(_column_definitions.size());
  for (const auto& column_definition : _column_definitions) {
    types.emplace_back(column_definition.data_type);
  }
  return types;
}

bool Table::column_is_nullable(const ColumnID column_id) const {
  Assert(column_id < _column_definitions.size(), "ColumnID out of range.");
  return _column_definitions[column_id].nullable;
}

std::vector<bool> Table::columns_are_nullable() const {
  auto nullable = std::vector<bool>(column_count());
  for (auto column_id = ColumnID{0}; column_id < column_count(); ++column_id) {
    nullable[column_id] = _column_definitions[column_id].nullable;
  }
  return nullable;
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  const auto iter = std::find_if(_column_definitions.begin(), _column_definitions.end(),
                                 [&](const auto& column_definition) { return column_definition.name == column_name; });
  Assert(iter != _column_definitions.end(), "Couldn't find column '" + column_name + "'.");
  return ColumnID{static_cast<ColumnID::base_type>(std::distance(_column_definitions.begin(), iter))};
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  auto last_chunk = !_chunks.empty() ? get_chunk(ChunkID{chunk_count() - 1}) : nullptr;
  if (!last_chunk || last_chunk->size() >= _target_chunk_size || !last_chunk->is_mutable()) {
    // One chunk reached its capacity and was not finalized before.
    if (last_chunk && last_chunk->is_mutable()) {
      last_chunk->finalize();
    }

    append_mutable_chunk();
    last_chunk = get_chunk(ChunkID{chunk_count() - 1});
  }

  last_chunk->append(values);
}

void Table::append_mutable_chunk() {
  auto segments = Segments{};
  for (const auto& column_definition : _column_definitions) {
    resolve_data_type(column_definition.data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      segments.push_back(
          std::make_shared<ValueSegment<ColumnDataType>>(column_definition.nullable, _target_chunk_size));
    });
  }

  auto mvcc_data = std::shared_ptr<MvccData>{};
  if (_use_mvcc == UseMvcc::Yes) {
    mvcc_data = std::make_shared<MvccData>(_target_chunk_size, MvccData::MAX_COMMIT_ID);
  }

  append_chunk(segments, mvcc_data);
}

uint64_t Table::row_count() const {
  if (_type == TableType::References && _cached_row_count && !HYRISE_DEBUG) {
    return *_cached_row_count;
  }

  auto row_count = uint64_t{0};
  const auto chunk_count = _chunks.size();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = get_chunk(chunk_id);
    if (chunk) {
      row_count += chunk->size();
    }
  }

  if (_type == TableType::References) {
    // After being created, reference tables should never be changed again.
    DebugAssert(!_cached_row_count || row_count == *_cached_row_count, "Size of reference table has changed");

    // row_count() is called by AbstractOperator after the operator has finished to fill the performance data. As such,
    // no synchronization is necessary.
    _cached_row_count = row_count;
  }

  return row_count;
}

bool Table::empty() const {
  return row_count() == 0;
}

ChunkID Table::chunk_count() const {
  return ChunkID{static_cast<ChunkID::base_type>(_chunks.size())};
}

ChunkOffset Table::target_chunk_size() const {
  DebugAssert(_type == TableType::Data, "target_chunk_size is only valid for data tables");
  return _target_chunk_size;
}

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  if (_type == TableType::References) {
    // Not written concurrently, since reference tables are not modified anymore once they are written.
    return _chunks[chunk_id];
  }

  return std::atomic_load(&_chunks[chunk_id]);
}

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  if (_type == TableType::References) {
    // see comment in non-const function
    return _chunks[chunk_id];
  }

  return std::atomic_load(&_chunks[chunk_id]);
}

std::shared_ptr<Chunk> Table::last_chunk() const {
  DebugAssert(!_chunks.empty(), "last_chunk() called on Table without chunks");
  if (_type == TableType::References) {
    // Not written concurrently, since reference tables are not modified anymore once they are written.
    return _chunks.back();
  }

  return std::atomic_load(&_chunks.back());
}

void Table::remove_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID " + std::to_string(chunk_id) + " out of range");
  DebugAssert(([this, chunk_id]() {  // NOLINT
                const auto chunk = get_chunk(chunk_id);
                return (chunk->invalid_row_count() == chunk->size());
              }()),
              "Physical delete of chunk prevented: Chunk needs to be fully invalidated before.");
  Assert(_type == TableType::Data, "Removing chunks from other tables than data tables is not intended yet.");
  std::atomic_store(&_chunks[chunk_id], std::shared_ptr<Chunk>(nullptr));
}

void Table::append_chunk(const Segments& segments, std::shared_ptr<MvccData> mvcc_data,  // NOLINT
                         const std::optional<PolymorphicAllocator<Chunk>>& alloc) {
  Assert(_type != TableType::Data || static_cast<bool>(mvcc_data) == (_use_mvcc == UseMvcc::Yes),
         "Supply MvccData to data Tables if MVCC is enabled.");
  AssertInput(static_cast<ColumnCount::base_type>(segments.size()) == column_count(),
              "Input does not have the same number of columns.");

  if constexpr (HYRISE_DEBUG) {
    for (const auto& segment : segments) {
      const auto is_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment) != nullptr;
      Assert(is_reference_segment == (_type == TableType::References), "Invalid Segment type");
    }

    // Check that existing chunks are not empty
    const auto chunk_count = _chunks.size();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = get_chunk(chunk_id);
      if (!chunk) {
        continue;
      }

      // An empty, mutable chunk at the end is fine, but in that case, append_chunk shouldn't have to be called.
      DebugAssert(chunk->size() > 0, "append_chunk called on a table that has an empty chunk");
    }
  }

  // tbb::concurrent_vector does not guarantee that elements reported by size() are fully initialized yet.
  // https://oneapi-src.github.io/oneTBB/main/tbb_userguide/concurrent_vector_ug.html
  // To avoid someone reading an incomplete shared_ptr<Chunk>, we (1) use the ZeroAllocator for the concurrent_vector,
  // making sure that an uninitialized entry compares equal to nullptr and (2) insert the desired chunk atomically.

  auto new_chunk_iter = _chunks.push_back(nullptr);
  std::atomic_store(&*new_chunk_iter, std::make_shared<Chunk>(segments, mvcc_data, alloc));
}

std::vector<AllTypeVariant> Table::get_row(size_t row_idx) const {
  PerformanceWarning("get_row() used");
  const auto chunk_count = _chunks.size();
  const auto num_columns = column_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }

    if (row_idx < chunk->size()) {
      auto row = std::vector<AllTypeVariant>(num_columns);

      for (auto column_id = ColumnID{0}; column_id < num_columns; ++column_id) {
        row[column_id] = chunk->get_segment(column_id)->operator[](static_cast<ChunkOffset>(row_idx));
      }

      return row;
    }

    row_idx -= chunk->size();
  }

  Fail("row_idx out of bounds");
}

std::vector<std::vector<AllTypeVariant>> Table::get_rows() const {
  PerformanceWarning("get_rows() used");

  // Allocate all rows
  auto rows = std::vector<std::vector<AllTypeVariant>>{row_count()};
  const auto num_columns = column_count();
  for (auto& row : rows) {
    row.resize(num_columns);
  }

  // Materialize the Chunks
  auto chunk_begin_row_idx = size_t{0};
  const auto chunk_count = _chunks.size();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }

    for (auto column_id = ColumnID{0}; column_id < num_columns; ++column_id) {
      segment_iterate(*chunk->get_segment(column_id), [&](const auto& segment_position) {
        if (!segment_position.is_null()) {
          rows[chunk_begin_row_idx + segment_position.chunk_offset()][column_id] = segment_position.value();
        }
      });
    }

    chunk_begin_row_idx += chunk->size();
  }

  return rows;
}

std::unique_lock<std::mutex> Table::acquire_append_mutex() {
  return std::unique_lock<std::mutex>(*_append_mutex);
}

std::shared_ptr<TableStatistics> Table::table_statistics() const {
  return _table_statistics;
}

void Table::set_table_statistics(const std::shared_ptr<TableStatistics>& table_statistics) {
  _table_statistics = table_statistics;
}

std::vector<ChunkIndexStatistics> Table::chunk_indexes_statistics() const {
  return _chunk_indexes_statistics;
}

std::vector<TableIndexStatistics> Table::table_indexes_statistics() const {
  return _table_indexes_statistics;
}

template <typename Index>
void Table::create_chunk_index(const std::vector<ColumnID>& column_ids, const std::string& name) {
  static_assert(std::is_base_of<AbstractChunkIndex, Index>::value,
                "'Index' template argument is not an AbstractChunkIndex.");

  const auto chunk_index_type = get_chunk_index_type_of<Index>();

  const auto chunk_count = _chunks.size();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = get_chunk(chunk_id);
    Assert(chunk, "Requested index on deleted chunk.");
    Assert(!chunk->is_mutable(), "Cannot index mutable chunk.");
    chunk->create_index<Index>(column_ids);
  }
  _chunk_indexes_statistics.emplace_back(ChunkIndexStatistics{column_ids, name, chunk_index_type});
}

void Table::add_soft_key_constraint(const TableKeyConstraint& table_key_constraint) {
  Assert(_type == TableType::Data, "TableKeyConstraints are not tracked for reference tables across the PQP.");

  // Check validity of specified columns.
  const auto column_count = this->column_count();
  for (const auto& column_id : table_key_constraint.columns()) {
    Assert(column_id < column_count, "ColumnID out of range.");

    // PRIMARY KEY requires non-nullable columns.
    if (table_key_constraint.key_type() == KeyConstraintType::PRIMARY_KEY) {
      Assert(!column_is_nullable(column_id), "Column must be non-nullable to comply with PRIMARY KEY.");
    }
  }

  const auto append_lock = acquire_append_mutex();

  for (const auto& existing_constraint : _table_key_constraints) {
    // Ensure that no other PRIMARY KEY is defined.
    Assert(existing_constraint.key_type() == KeyConstraintType::UNIQUE ||
               table_key_constraint.key_type() == KeyConstraintType::UNIQUE,
           "Another primary TableKeyConstraint exists for this table.");

    // Ensure there is only one key constraint per column(s). Theoretically, there could be two unique constraints
    // {a, b} and {b, c}, but for now we prohibit these cases.
    Assert(!columns_intersect(existing_constraint.columns(), table_key_constraint.columns()),
           "Another TableKeyConstraint for the same column(s) has already been defined.");
  }

  _table_key_constraints.insert(table_key_constraint);
}

const TableKeyConstraints& Table::soft_key_constraints() const {
  return _table_key_constraints;
}

void Table::add_soft_foreign_key_constraint(const ForeignKeyConstraint& foreign_key_constraint) {
  Assert(_type == TableType::Data, "ForeignKeyConstraints are not tracked for reference tables across the PQP.");

  Assert(&*foreign_key_constraint.foreign_key_table() == this, "ForeignKeyConstraint is added to the wrong table.");

  // Check validity of specified columns.
  const auto column_count = this->column_count();
  for (const auto& column_id : foreign_key_constraint.foreign_key_columns()) {
    Assert(column_id < column_count, "ColumnID out of range.");
  }

  const auto referenced_table = foreign_key_constraint.primary_key_table();
  Assert(referenced_table && &*referenced_table != this, "ForeignKeyConstraint must reference another existing table.");

  // Check validity of key columns from other table.
  const auto referenced_table_column_count = referenced_table->column_count();
  for (const auto& column_id : foreign_key_constraint.primary_key_columns()) {
    Assert(column_id < referenced_table_column_count, "ColumnID out of range.");
  }

  const auto append_lock = acquire_append_mutex();
  for (const auto& existing_constraint : _foreign_key_constraints) {
    // Do not allow intersecting foreign key constraints. Though a table may have unlimited inclusion dependencies for
    // the same columns (and especially the existence of [a] in [x] and [b] in [y] does not mean [a, b] in [x, y]
    // holds), it is reasonable to assume disjoint (soft) foreign keys for now.
    Assert(!columns_intersect(existing_constraint.foreign_key_columns(), foreign_key_constraint.foreign_key_columns()),
           "ForeignKeyConstraint for required columns has already been set.");
  }
  _foreign_key_constraints.insert(foreign_key_constraint);
  const auto referenced_table_append_lock = referenced_table->acquire_append_mutex();
  referenced_table->_referenced_foreign_key_constraints.insert(foreign_key_constraint);
}

const ForeignKeyConstraints& Table::soft_foreign_key_constraints() const {
  return _foreign_key_constraints;
}

const ForeignKeyConstraints& Table::referenced_foreign_key_constraints() const {
  return _referenced_foreign_key_constraints;
}

void Table::add_soft_order_constraint(const TableOrderConstraint& table_order_constraint) {
  Assert(_type == TableType::Data, "TableOrderConstraints are not tracked for reference tables across the PQP.");

  // Check validity of columns.
  const auto column_count = this->column_count();
  for (const auto& column_id : table_order_constraint.ordering_columns()) {
    Assert(column_id < column_count, "ColumnID out of range.");
  }

  // Check validity of ordered columns.
  for (const auto& column_id : table_order_constraint.ordered_columns()) {
    Assert(column_id < column_count, "ColumnID out of range.");
  }

  const auto append_lock = acquire_append_mutex();
  for (const auto& existing_constraint : _table_order_constraints) {
    // Do not allow intersecting key constraints. Though they can be valid, we are pessimistic for now and notice if we
    // run into intricate cases.
    const auto ordering_columns_invalid =
        columns_intersect(existing_constraint.ordering_columns(), table_order_constraint.ordering_columns()) &&
        existing_constraint.ordered_columns() == table_order_constraint.ordered_columns();
    const auto ordered_columns_invalid =
        columns_intersect(existing_constraint.ordered_columns(), table_order_constraint.ordered_columns()) &&
        existing_constraint.ordering_columns() == table_order_constraint.ordering_columns();
    Assert(!ordering_columns_invalid && !ordered_columns_invalid,
           "TableOrderConstraint for required columns has already been set.");
  }
  _table_order_constraints.insert(table_order_constraint);
}

const TableOrderConstraints& Table::soft_order_constraints() const {
  return _table_order_constraints;
}

const std::vector<ColumnID>& Table::value_clustered_by() const {
  return _value_clustered_by;
}

void Table::set_value_clustered_by(const std::vector<ColumnID>& value_clustered_by) {
  // Ensure that all chunks are finalized because the table should not be altered afterwards.
  const auto chunk_count = _chunks.size();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }

    Assert(!get_chunk(chunk_id)->is_mutable(), "Cannot set value_clustering on table with mutable chunks");
  }

  if constexpr (HYRISE_DEBUG) {
    if (chunk_count > 1) {
      for (const auto& column_id : value_clustered_by) {
        resolve_data_type(_column_definitions[column_id].data_type, [&](const auto column_data_type) {
          using ColumnDataType = typename decltype(column_data_type)::type;

          auto value_to_chunk_map = std::unordered_map<ColumnDataType, ChunkID>{};
          for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
            const auto& chunk = get_chunk(chunk_id);
            const auto& segment = chunk->get_segment(column_id);
            segment_iterate<ColumnDataType>(*segment, [&](const auto position) {
              Assert(!position.is_null(), "Value clustering is not defined for columns storing NULLs.");

              const auto& [iter, inserted] = value_to_chunk_map.try_emplace(position.value(), chunk_id);
              if (!inserted) {
                Assert(iter->second == chunk_id,
                       "Table cannot be set to value-clustered as same value "
                       "is found in more than one chunk");
              }
            });
          }
        });
      }
    }
  }

  _value_clustered_by = value_clustered_by;
}

pmr_vector<std::shared_ptr<PartialHashIndex>> Table::get_table_indexes() const {
  return _table_indexes;
}

std::vector<std::shared_ptr<PartialHashIndex>> Table::get_table_indexes(const ColumnID column_id) const {
  auto result = std::vector<std::shared_ptr<PartialHashIndex>>();
  std::copy_if(_table_indexes.cbegin(), _table_indexes.cend(), std::back_inserter(result),
               [&](const auto& index) { return index->is_index_for(column_id); });
  return result;
}

size_t Table::memory_usage(const MemoryUsageCalculationMode mode) const {
  auto bytes = size_t{sizeof(*this)};

  const auto chunk_count = _chunks.size();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }

    bytes += chunk->memory_usage(mode);
  }

  for (const auto& column_definition : _column_definitions) {
    bytes += column_definition.name.size();
  }

  // TODO(anybody) Statistics and Indexes missing from Memory Usage Estimation
  // TODO(anybody) TableLayout missing

  return bytes;
}

void Table::create_partial_hash_index(const ColumnID column_id, const std::vector<ChunkID>& chunk_ids) {
  if (chunk_ids.empty()) {
    Fail("Creating a partial hash index with no chunks being indexed is not supported.");
  }

  auto table_index = std::shared_ptr<PartialHashIndex>{};
  auto chunks_to_index = std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>{};

  chunks_to_index.reserve(chunk_ids.size());
  for (const auto chunk_id : chunk_ids) {
    const auto& chunk = get_chunk(chunk_id);
    Assert(chunk, "Requested index on deleted chunk.");
    Assert(!chunk->is_mutable(), "Cannot index mutable chunk.");
    chunks_to_index.emplace_back(chunk_id, chunk);
  }

  table_index = std::make_shared<PartialHashIndex>(chunks_to_index, column_id);

  _table_indexes.emplace_back(table_index);

  _table_indexes_statistics.emplace_back(TableIndexStatistics{{column_id}, chunks_to_index});
}

template void Table::create_chunk_index<GroupKeyIndex>(const std::vector<ColumnID>& column_ids,
                                                       const std::string& name);
template void Table::create_chunk_index<CompositeGroupKeyIndex>(const std::vector<ColumnID>& column_ids,
                                                                const std::string& name);
template void Table::create_chunk_index<AdaptiveRadixTreeIndex>(const std::vector<ColumnID>& column_ids,
                                                                const std::string& name);

}  // namespace hyrise
