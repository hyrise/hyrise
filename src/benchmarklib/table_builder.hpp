#pragma once

#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "boost/hana/assert.hpp"
#include "boost/hana/for_each.hpp"
#include "boost/hana/tuple.hpp"
#include "boost/hana/zip_with.hpp"

#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

// helper structs and functions for compile time, the actual code is in namespace opossum
namespace table_builder {
// similar to std::optional but has_value is known at compile time, so "if constexpr" can be used
// boost::hana::optional does not allow moving and reinitializing its value (or I just did not find out how)
template <typename T, bool _has_value, typename Enable = void>
class OptionalConstexpr {
 public:
  template <typename... Args>
  explicit OptionalConstexpr(Args&&... args) : _value{std::forward<Args>(args)...} {}

  static_assert(_has_value);
  static constexpr bool has_value = true;

  T& value() { return _value; }

 private:
  T _value;
};

template <typename T, bool _has_value>
class OptionalConstexpr<T, _has_value, std::enable_if_t<!_has_value>> {
 public:
  template <typename... Args>
  explicit OptionalConstexpr(Args&&...) {}

  static_assert(!_has_value);
  static constexpr bool has_value = false;

  T& value() {
    Assert(false, "empty optional has no value");
    return {};
  }
};

template <typename T, typename Enable = void>
struct is_optional : std::false_type {};

template <typename T>
struct is_optional<T, std::enable_if_t<std::is_same_v<T, std::optional<typename T::value_type>>>> : std::true_type {};

// is_optional_v<T> <=> T is of type std::optional<?>
template <typename T>
constexpr bool is_optional_v = is_optional<T>::value;

template <typename T, typename Enable = void>
struct GetValueType {
  using value_type = T;
};

template <typename T>
struct GetValueType<T, std::enable_if_t<is_optional_v<T>>> {
  using value_type = typename T::value_type;
};

// get the value_type (lets call it V) of std::optional<V> and V using the same syntax - needed for templating
template <typename T>
using get_value_type = typename GetValueType<T>::value_type;

// check if std::optional<V> or V is null using the same syntax - needed for templating
template <typename T>
constexpr bool is_null(const T& optional_or_value) {
  if constexpr (is_optional_v<T>) {
    return !optional_or_value.has_value();
  } else {
    return false;
  }
}

// get the value of std::optional<V> or V using the same syntax - needed for templating
template <typename T>
get_value_type<T>& get_value(T& optional_or_value) {
  if constexpr (is_optional_v<T>) {
    return optional_or_value.value();
  } else {
    return optional_or_value;
  }
}

}  // namespace table_builder

/**
 * Helper to build a table with a static column layout, specified by constructor arguments types and names. Keeps a
 * value vector for each column and appends values to them in append_row(). For nullable columns an additional
 * null_values vector is kept. Automatically creates chunks in accordance with the specified chunk size.
 */
template <typename... DataTypes>
class TableBuilder {
 public:
  // names is a boost::hana::tuple of strings defining column names
  // types is a list of equal length defining respective column types
  // types may contain std::optional<?>, which will result in a nullable column, otherwise columns are not nullable
  template <typename Names>
  TableBuilder(const ChunkOffset chunk_size, const boost::hana::tuple<DataTypes...>& types, const Names& names,
               const ChunkOffset estimated_rows = 0)
      : _estimated_rows_per_chunk(std::min(estimated_rows, chunk_size)), _row_count{0} {
    BOOST_HANA_CONSTANT_ASSERT(boost::hana::size(names) == boost::hana::size(types));

    // Iterate over the column types/names and create the columns.
    auto column_definitions = TableColumnDefinitions{};
    boost::hana::for_each(boost::hana::zip(names, types), [&](const auto& name_and_type) {
      auto name = name_and_type[boost::hana::llong_c<0>];
      auto type = name_and_type[boost::hana::llong_c<1>];

      auto data_type = data_type_from_type<table_builder::get_value_type<decltype(type)>>();
      auto is_nullable = table_builder::is_optional_v<decltype(type)>;

      column_definitions.emplace_back(name, data_type, is_nullable);
    });
    _table = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, UseMvcc::Yes);

    // Reserve some space in the vectors
    boost::hana::for_each(_value_vectors, [&](auto& values) { values.reserve(_estimated_rows_per_chunk); });
    boost::hana::for_each(_null_value_vectors, [&](auto& null_values) {
      if constexpr (std::decay_t<decltype(null_values)>::has_value) {
        null_values.value().reserve(_estimated_rows_per_chunk);
      }
    });
  }

  std::shared_ptr<Table> finish_table() {
    if (_current_chunk_row_count() > 0) {
      _emit_chunk();
    }

    return _table;
  }

  template <typename... Types>
  void append_row(Types&&... new_values) {
    auto values_tuple = boost::hana::make_tuple(std::forward<Types>(new_values)...);
    BOOST_HANA_CONSTANT_ASSERT(boost::hana::size(_value_vectors) == boost::hana::size(values_tuple));

    // Create tuples ([&values0, &null_values0, new_value0], [&values1, &null_values1, new_value1], ...)
    auto value_vectors_and_null_value_vectors_and_values = boost::hana::zip_with(
        [](auto& values, auto& null_values, auto&& value) {
          return boost::hana::make_tuple(std::reference_wrapper(values), std::reference_wrapper(null_values),
                                         std::forward<decltype(value)>(value));
        },
        _value_vectors, _null_value_vectors, values_tuple);

    // Add the values to their respective value vector
    boost::hana::for_each(value_vectors_and_null_value_vectors_and_values, [](auto& values_and_null_values_and_value) {
      auto& values = values_and_null_values_and_value[boost::hana::llong_c<0>].get();
      auto& null_values = values_and_null_values_and_value[boost::hana::llong_c<1>].get();

      // the type of optional_or_value is either std::optional<T> or just T, hence the variable name
      auto& optional_or_value = values_and_null_values_and_value[boost::hana::llong_c<2>];

      constexpr auto column_is_nullable = std::decay_t<decltype(null_values)>::has_value;
      auto value_is_null = table_builder::is_null(optional_or_value);

      DebugAssert(column_is_nullable || !value_is_null, "cannot insert null value into not-null-column");

      if (value_is_null) {
        values.emplace_back();
      } else {
        values.emplace_back(std::move(table_builder::get_value(optional_or_value)));
      }

      if constexpr (column_is_nullable) {
        null_values.value().emplace_back(value_is_null);
      }
    });

    _row_count++;

    // The reason why we can't put the for_each lambda's code into zip_with's lambda is the following:
    // boost::hana's higher order functions (like zip_with) do not guarantee the order of execution of a passed
    // function f or even the number of executions of f. Therefore it should only be used with pure functions (no side
    // effects). There are exceptions to that, for_each gives these guarantees and expects impure functions.

    if (_current_chunk_row_count() >= _table->max_chunk_size()) {
      _emit_chunk();
    }
  }

  size_t row_count() const { return _row_count; }

 private:
  std::shared_ptr<Table> _table;
  ChunkOffset _estimated_rows_per_chunk;

  // _table->row_count() only counts completed chunks but we want the total number of rows added to this table builder
  size_t _row_count;

  boost::hana::tuple<std::vector<table_builder::get_value_type<DataTypes>>...> _value_vectors;
  boost::hana::tuple<table_builder::OptionalConstexpr<std::vector<bool>, (table_builder::is_optional<DataTypes>())>...>
      _null_value_vectors;

  size_t _current_chunk_row_count() const { return _value_vectors[boost::hana::llong_c<0>].size(); }

  void _emit_chunk() {
    auto segments = Segments{};

    auto _value_vectors_and_null_value_vectors = boost::hana::zip_with(
        [](auto& values, auto& null_values) {
          return boost::hana::make_tuple(std::reference_wrapper(values), std::reference_wrapper(null_values));
        },
        _value_vectors, _null_value_vectors);

    // Create a segment from each value vector and add it to the Chunk, then re-initialize the vector
    boost::hana::for_each(_value_vectors_and_null_value_vectors, [&](auto& values_and_null_values) {
      auto& values = values_and_null_values[boost::hana::llong_c<0>].get();
      auto& null_values = values_and_null_values[boost::hana::llong_c<1>].get();

      using T = typename std::decay_t<decltype(values)>::value_type;
      if constexpr (std::decay_t<decltype(null_values)>::has_value) {  // column is nullable
        segments.emplace_back(std::make_shared<ValueSegment<T>>(std::move(values), std::move(null_values.value())));

        null_values.value() = std::vector<bool>{};
        null_values.value().reserve(_estimated_rows_per_chunk);
      } else {
        segments.emplace_back(std::make_shared<ValueSegment<T>>(std::move(values)));
      }

      values = std::decay_t<decltype(values)>{};
      values.reserve(_estimated_rows_per_chunk);
    });

    auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});

    _table->append_chunk(segments, mvcc_data);
  }
};

}  // namespace opossum
