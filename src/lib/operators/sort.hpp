#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

// operator to sort a table by a single column
// Multi-column sort is not supported yet. For now, you will have to sort by the secondary criterion, then by the first

// The parameter chunk_size sets the chunk size of the output table, wich will allways be materialized
class Sort : public AbstractReadOnlyOperator {
 public:
  Sort(const std::shared_ptr<const AbstractOperator> in, const std::string &sort_column_name,
       const bool ascending = true, const size_t output_chunk_size = 0);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  std::shared_ptr<const Table> on_execute() override;

  // The operator is seperated in three different classes. SortImpl is the common templated implementation of the
  // operator. SortImpl* und SortImplMaterializeOutput are extra classes for the visitor pattern. They fulfill a certain
  // task during the Sort process, as described later on.
  template <typename SortColumnType>
  class SortImpl;
  template <typename SortColumnType>
  class SortImplMaterializeSortColumn;
  template <typename SortColumnType>
  class SortImplMaterializeOutput;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;
  const std::string _sort_column_name;
  const bool _ascending;
  const size_t _output_chunk_size;
};

// we need to use the impl pattern because the scan operator of the sort depends on the type of the column
template <typename SortColumnType>
class Sort::SortImpl : public AbstractReadOnlyOperatorImpl {
 public:
  SortImpl(const std::shared_ptr<const Table> table_in, const std::string &sort_column_name,
           const bool ascending = true, const size_t output_chunk_size = 0)
      : _table_in(table_in),
        _sort_column_name(sort_column_name),
        _ascending(ascending),
        _output_chunk_size(output_chunk_size) {
    // initialize a structure wich can be sorted by std::sort
    _row_id_value_vector = std::make_shared<std::vector<std::pair<RowID, SortColumnType>>>();
  }

  std::shared_ptr<const Table> on_execute() override {
    // 1. Prepare Sort: Creating rowid-value-Structur
    auto preparation = std::make_shared<SortImplMaterializeSortColumn<SortColumnType>>(_table_in, _sort_column_name,
                                                                                       _row_id_value_vector);
    preparation->execute();

    // 2. After we got our ValueRowID Map we sort the map by the value of the pair
    if (_ascending) {
      sort_with_operator<std::less<>>();
    } else {
      sort_with_operator<std::greater<>>();
    }

    // 3. Materialization of the result: We take the sorted ValueRowID Vector, create chunks fill them until they are
    // full and create the next one. Each chunk is filled row by row.
    auto materialization = std::make_shared<SortImplMaterializeOutput<SortColumnType>>(_table_in, _row_id_value_vector,
                                                                                       _output_chunk_size);
    return materialization->execute();
  }

  template <typename Comp>
  void sort_with_operator() {
    Comp comp;
    std::stable_sort(_row_id_value_vector->begin(), _row_id_value_vector->end(),
                     [comp](std::pair<RowID, SortColumnType> a, std::pair<RowID, SortColumnType> b) {
                       return comp(a.second, b.second);
                     });
  }

  const std::shared_ptr<const Table> _table_in;

  // column to sort by
  const std::string _sort_column_name;
  const bool _ascending;
  // chunk size of the materialized output
  const size_t _output_chunk_size;

  std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> _row_id_value_vector;
};

// This class fills the temporary structure to be sorted. Therefore the column to sort by is visited by this class, so
// the values can be copied in the temporary row_id_value vector.
template <typename SortColumnType>
class Sort::SortImplMaterializeSortColumn : public ColumnVisitable {
 public:
  SortImplMaterializeSortColumn(std::shared_ptr<const Table> in, const std::string &sort_column_name,
                                std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> id_value_map)
      : _table_in(in), _sort_column_name(sort_column_name), _row_id_value_vector(id_value_map) {}

  struct MaterializeSortColumnContext : ColumnVisitableContext {
    MaterializeSortColumnContext(ChunkID c, std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> id_value_map)
        : chunk_id(c), row_id_value_vector(id_value_map) {}

    // constructor for use in ReferenceColumn::visit_dereferenced
    MaterializeSortColumnContext(std::shared_ptr<ColumnVisitableContext> base_context, ChunkID chunk_id,
                                 std::shared_ptr<std::vector<ChunkOffset>> chunk_offsets,
                                 std::shared_ptr<std::vector<RowID>> row_ids)
        : chunk_id(chunk_id),
          row_id_value_vector(
              std::static_pointer_cast<MaterializeSortColumnContext>(base_context)->row_id_value_vector),
          chunk_offsets_in(chunk_offsets),
          row_ids_in(row_ids) {}

    const ChunkID chunk_id;
    std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> row_id_value_vector;
    std::shared_ptr<std::vector<ChunkOffset>> chunk_offsets_in;
    std::shared_ptr<std::vector<RowID>> row_ids_in;
  };

  void execute() {
    _row_id_value_vector->reserve(_table_in->row_count());
    auto sort_column_id = _table_in->column_id_by_name(_sort_column_name);

    for (ChunkID chunk_id = 0; chunk_id < _table_in->chunk_count(); chunk_id++) {
      _table_in->get_chunk(chunk_id)
          .get_column(sort_column_id)
          ->visit(*this, std::make_shared<MaterializeSortColumnContext>(chunk_id, _row_id_value_vector));
    }
  }

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<MaterializeSortColumnContext>(base_context);
    const auto &column = static_cast<ValueColumn<SortColumnType> &>(base_column);
    auto values = column.values();
    auto chunk_id = context->chunk_id;
    auto row_id_value_vector = context->row_id_value_vector;

    if (context->chunk_offsets_in) {
      // This ValueColumn is referenced by a ReferenceColumn (i.e., is probably filtered). We only return the matching
      // rows within the filtered column, together with their original position
      auto chunk_offsets = context->chunk_offsets_in;
      auto row_ids = context->row_ids_in;

      for (size_t i = 0; i < chunk_offsets->size(); i++) {
        row_id_value_vector->emplace_back(row_ids->at(i), values[chunk_offsets->at(i)]);
      }
    } else {
      // This ValueColumn has to be scanned in full. We directly insert the results into the list of matching rows.
      for (ChunkOffset row_index = 0; row_index < values.size(); row_index++) {
        row_id_value_vector->emplace_back(RowID{chunk_id, row_index}, values[row_index]);
      }
    }
  }

  void handle_reference_column(ReferenceColumn &column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    /*
    The pos_list might be unsorted. In that case, we would have to jump around from chunk to chunk.
    One-chunk-at-a-time processing should be faster. For this, we place a pair {chunk_offset, original_position}
    into a vector for each chunk. A potential optimization would be to only do this if the pos_list is really
    unsorted.
    */
    auto referenced_table = column.referenced_table();
    std::vector<std::shared_ptr<std::vector<ChunkOffset>>> all_chunk_offsets(referenced_table->chunk_count());
    std::vector<std::shared_ptr<std::vector<RowID>>> row_id_maps(referenced_table->chunk_count());

    for (ChunkID chunk_id = 0; chunk_id < referenced_table->chunk_count(); ++chunk_id) {
      all_chunk_offsets[chunk_id] = std::make_shared<std::vector<ChunkOffset>>();
      row_id_maps[chunk_id] = std::make_shared<std::vector<RowID>>();
      for (ChunkOffset row_index = 0; row_index < column.size(); row_index++) {
        row_id_maps[chunk_id]->emplace_back(RowID{chunk_id, row_index});
      }
    }

    auto pos_list = column.pos_list();
    auto referenced_column_id = column.referenced_column_id();

    for (auto pos : *(pos_list)) {
      auto chunk_info = referenced_table->locate_row(pos);
      all_chunk_offsets[chunk_info.first]->emplace_back(chunk_info.second);
    }

    for (ChunkID chunk_id = 0; chunk_id < referenced_table->chunk_count(); ++chunk_id) {
      if (all_chunk_offsets[chunk_id]->empty()) continue;
      auto &chunk = referenced_table->get_chunk(chunk_id);
      auto referenced_column = chunk.get_column(referenced_column_id);

      // visiting the referenced column with the created chunk_offset list
      auto c = std::make_shared<MaterializeSortColumnContext>(base_context, chunk_id, all_chunk_offsets[chunk_id],
                                                              row_id_maps[chunk_id]);
      referenced_column->visit(*this, c);
    }
  }

  void handle_dictionary_column(BaseColumn &base_column,
                                std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<MaterializeSortColumnContext>(base_context);
    const auto &column = static_cast<DictionaryColumn<SortColumnType> &>(base_column);
    auto attribute_vector = column.attribute_vector();
    auto dictionary = column.dictionary();
    auto chunk_id = context->chunk_id;
    auto row_id_value_vector = context->row_id_value_vector;

    if (context->chunk_offsets_in) {
      // This DictionaryColumn is referenced by a ReferenceColumn (i.e., is probably filtered). We only return the
      // matching
      // rows within the filtered column, together with their original position
      auto chunk_offsets = context->chunk_offsets_in;
      auto row_ids = context->row_ids_in;

      for (size_t i = 0; i < chunk_offsets->size(); i++) {
        auto value = column.value_by_value_id(attribute_vector->get(chunk_offsets->at(i)));
        row_id_value_vector->emplace_back(row_ids->at(i), value);
      }
    } else {
      // This DictionaryColumn has to be scanned in full. We directly insert the results into the list of matching rows.
      for (ChunkOffset row_index = 0; row_index < attribute_vector->size(); row_index++) {
        auto value = column.value_by_value_id(attribute_vector->get(row_index));
        row_id_value_vector->emplace_back(RowID{chunk_id, row_index}, value);
      }
    }
  }

  const std::shared_ptr<const Table> _table_in;
  const std::string _sort_column_name;
  std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> _row_id_value_vector;
};

// This class fulfills only the materialization task for a sorted row_id_value_vector.
template <typename SortColumnType>
class Sort::SortImplMaterializeOutput {
 public:
  // creates a new table with reference columns
  SortImplMaterializeOutput(std::shared_ptr<const Table> in,
                            std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> id_value_map,
                            const size_t output_chunk_size)
      : _table_in(in), _output_chunk_size(output_chunk_size), _row_id_value_vector(id_value_map) {}

  std::shared_ptr<const Table> execute() {
    // First we create a new table as the output
    auto output = std::make_shared<Table>(_output_chunk_size);

    for (size_t column_id = 0; column_id < _table_in->col_count(); column_id++) {
      output->add_column_definition(_table_in->column_name(column_id), _table_in->column_type(column_id));
    }

    // After we created the output table and initialized the column structure, we can start adding values. Because the
    // values are not ordered by input chunks anymore, we can't process them chunk by chunk. Instead the values are
    // copied column by column for each output row. For each column in a row we visit the input column with a reference
    // to the output column. This enables for the SortImplMaterializeOutput class to ignore the column types during the
    // copying of the values.
    ChunkID chunk_count_out;
    if (_output_chunk_size) {
      chunk_count_out = _row_id_value_vector->size() % _output_chunk_size
                            ? (_row_id_value_vector->size() / _output_chunk_size) + 1
                            : _row_id_value_vector->size() / _output_chunk_size;
    } else {
      chunk_count_out = 1;
      _output_chunk_size = _row_id_value_vector->size();
    }
    auto row_index = 0u;
    for (ChunkID chunk_id_out = 0; chunk_id_out < chunk_count_out; chunk_id_out++) {
      // Because we want to add the values row wise we have to save all columns temporarily before we can add them to
      // the
      // output chunk.
      auto column_vectors = std::vector<std::shared_ptr<BaseColumn>>(output->col_count());
      for (size_t column_id = 0; column_id < output->col_count(); column_id++) {
        auto column_type = _table_in->column_type(column_id);
        column_vectors[column_id] = make_shared_by_column_type<BaseColumn, ValueColumn>(column_type);
      }

      Chunk chunk_out;
      // The last chunk might not be completely filled, so we have to check if we got out of range.
      for (ChunkOffset chunk_offset_out = 0;
           chunk_offset_out < _output_chunk_size && row_index < _row_id_value_vector->size(); chunk_offset_out++) {
        auto row_id_in = _table_in->locate_row(_row_id_value_vector->at(row_index).first);
        for (size_t column_id = 0; column_id < output->col_count(); column_id++) {
          // The actual value is added by visiting the input column, wich then calls a function on the output column to
          // copy the value

          auto column = _table_in->get_chunk(row_id_in.first).get_column(column_id);
          auto chunk_offset_in = row_id_in.second;
          if (auto reference_column = std::dynamic_pointer_cast<ReferenceColumn>(column)) {
            auto pos = reference_column->pos_list()->operator[](row_id_in.second);
            column = reference_column->referenced_table()
                         ->get_chunk(pos.chunk_id)
                         .get_column(reference_column->referenced_column_id());
            chunk_offset_in = pos.chunk_offset;
          }
          column->copy_value_to_value_column(*column_vectors[column_id], chunk_offset_in);
        }
        row_index++;
      }
      for (size_t column_id = 0; column_id < output->col_count(); column_id++) {
        chunk_out.add_column(std::move(column_vectors[column_id]));
      }
      output->add_chunk(std::move(chunk_out));
    }

    return output;
  }

  std::shared_ptr<const Table> _table_in;
  size_t _output_chunk_size;
  std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> _row_id_value_vector;
};

}  // namespace opossum
