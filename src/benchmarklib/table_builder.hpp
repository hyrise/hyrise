#pragma once

#include <utility>

#include "boost/hana/for_each.hpp"
#include "boost/hana/integral_constant.hpp"
#include "boost/hana/zip_with.hpp"

#include "benchmark_config.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

/**
 * Helper to build a table with a static (specified by template args `ColumnTypes`) column type layout. Keeps a vector
 * for each column and appends values to them in append_row(). Automatically creates chunks in accordance with the
 * specified chunk size.
 */
template <typename... DataTypes>
// NOLINTNEXTLINE(fuchsia-trailing-return) - clang-tidy does not like the template parameter list
class TableBuilder {
 public:
  template <typename... Strings>
  // NOLINTNEXTLINE(fuchsia-trailing-return) - clang-tidy does not like the template parameter list
  TableBuilder(size_t chunk_size, const boost::hana::tuple<DataTypes...>& column_types,
               const boost::hana::tuple<Strings...>& column_names, opossum::UseMvcc use_mvcc, size_t estimated_rows = 0)
      : _use_mvcc(use_mvcc), _estimated_rows_per_chunk(estimated_rows < chunk_size ? estimated_rows : chunk_size) {
    /**
     * Create a tuple ((column_name0, column_type0), (column_name1, column_type1), ...) so we can iterate over the
     * columns.
     * fold_left as below does this in order, I think boost::hana::zip_with() doesn't, which is why I'm doing two steps
     * here.
     */
    const auto column_names_and_data_types = boost::hana::zip_with(
        [&](auto column_type, auto column_name) {
          return boost::hana::make_tuple(column_name, opossum::data_type_from_type<decltype(column_type)>());
        },
        column_types, column_names);

    // Iterate over the column types/names and create the columns.
    opossum::TableColumnDefinitions column_definitions;
    boost::hana::fold_left(column_names_and_data_types, column_definitions,
                           [](auto& definitions, auto column_name_and_type) -> decltype(definitions) {
                             definitions.emplace_back(column_name_and_type[boost::hana::llong_c<0>],
                                                      column_name_and_type[boost::hana::llong_c<1>]);
                             return definitions;
                           });
    _table = std::make_shared<opossum::Table>(column_definitions, opossum::TableType::Data, chunk_size, use_mvcc);

    // Reserve some space in the vectors
    boost::hana::for_each(_data_vectors, [&](auto&& vector) { vector.reserve(_estimated_rows_per_chunk); });
  }

  std::shared_ptr<opossum::Table> finish_table() {
    if (_current_chunk_row_count() > 0) {
      _emit_chunk();
    }

    return _table;
  }

  void append_row(DataTypes&&... column_values) {
    // Create a tuple ([&data_vector0, value0], ...)
    auto vectors_and_values = boost::hana::zip_with(
        [](auto& vector, auto&& value) {
          return boost::hana::make_tuple(std::reference_wrapper(vector), std::forward<decltype(value)>(value));
        },
        _data_vectors, boost::hana::make_tuple(std::forward<DataTypes>(column_values)...));

    // Add the values to their respective data vector
    boost::hana::for_each(vectors_and_values, [](auto&& vector_and_value) {
      vector_and_value[boost::hana::llong_c<0>].get().emplace_back(
          std::move(vector_and_value[boost::hana::llong_c<1>]));
    });

    if (_current_chunk_row_count() >= _table->max_chunk_size()) {
      _emit_chunk();
    }
  }

 private:
  std::shared_ptr<opossum::Table> _table;
  opossum::UseMvcc _use_mvcc;
  boost::hana::tuple<std::vector<DataTypes>...> _data_vectors;
  size_t _estimated_rows_per_chunk;

  size_t _current_chunk_row_count() const { return _data_vectors[boost::hana::llong_c<0>].size(); }

  void _emit_chunk() {
    opossum::Segments segments;

    // Create a segment from each data vector and add it to the Chunk, then re-initialize the vector
    boost::hana::for_each(_data_vectors, [&](auto&& vector) {
      using T = typename std::decay_t<decltype(vector)>::value_type;
      // reason for nolint: clang-tidy wants this to be a forward, but that doesn't work
      segments.push_back(std::make_shared<opossum::ValueSegment<T>>(std::move(vector)));  // NOLINT
      vector = std::decay_t<decltype(vector)>();
      vector.reserve(_estimated_rows_per_chunk);
    });

    // Create initial MvccData if MVCC is enabled
    auto mvcc_data = std::shared_ptr<MvccData>{};
    if (_use_mvcc == UseMvcc::Yes) {
      mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
    }

    _table->append_chunk(segments, mvcc_data);
  }
};

}  // namespace opossum