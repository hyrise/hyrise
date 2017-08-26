#include "table_scan.hpp"

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "resolve_column_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/base_attribute_vector.hpp"
#include "storage/column_visitable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/iterables/attribute_vector_iterable.hpp"
#include "storage/iterables/constant_value_iterable.hpp"
#include "storage/iterables/dictionary_column_iterable.hpp"
#include "storage/iterables/null_value_vector_iterable.hpp"
#include "storage/iterables/reference_column_iterable.hpp"
#include "storage/iterables/value_column_iterable.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"
#include "utils/binary_operators.hpp"
#include "utils/static_if.hpp"

namespace opossum {

namespace hana = boost::hana;

/**
 * @brief The actual scan
 */
class Scan {
 public:
  Scan(const ChunkID chunk_id, PosList &matches_out) : _chunk_id{chunk_id}, _matches_out{matches_out} {}

  template <typename LeftIterator, typename RightIterator, typename Comparator>
  void operator()(const Comparator &comparator, LeftIterator left_it, LeftIterator left_end, RightIterator right_it) {
    for (; left_it != left_end; ++left_it, ++right_it) {
      const auto left = *left_it;
      const auto right = *right_it;

      if (left.is_null() || right.is_null()) continue;

      if (comparator(left.value(), right.value())) {
        _matches_out.push_back(RowID{_chunk_id, left.chunk_offset()});
      }
    }
  }

 private:
  const ChunkID _chunk_id;
  PosList &_matches_out;
};

class ColumnScanBase {
 public:
  ColumnScanBase(std::shared_ptr<const Table> in_table, const ColumnID left_column_id, const ScanType scan_type)
      : _in_table{in_table}, _left_column_id{left_column_id}, _scan_type{scan_type} {}

  virtual ~ColumnScanBase() = default;

  virtual PosList scan_chunk(const ChunkID &chunk_id) = 0;

 protected:
  template <typename Type>
  static auto _create_iterable_from_column(ValueColumn<Type> &column) {
    return ValueColumnIterable<Type>{column};
  }

  template <typename Type>
  static auto _create_iterable_from_column(DictionaryColumn<Type> &column) {
    return DictionaryColumnIterable<Type>{column};
  }

  template <typename Type>
  static auto _create_iterable_from_column(ReferenceColumn &column) {
    return ReferenceColumnIterable<Type>{column};
  }

 protected:
  const std::shared_ptr<const Table> _in_table;
  const ColumnID _left_column_id;
  const ScanType _scan_type;
};

/**
 * @brief Resolves reference columns
 *
 * The position list of reference columns is split by the referenced columns and
 * then each one is visited separately.
 */
class SingleColumnScanBase : public ColumnScanBase, public ColumnVisitable {
 protected:
  struct Context : public ColumnVisitableContext {
    Context(const ChunkID chunk_id, PosList &matches_out) : _chunk_id{chunk_id}, _matches_out{matches_out} {}

    Context(const ChunkID chunk_id, PosList &matches_out,
            std::unique_ptr<std::vector<std::pair<ChunkOffset, ChunkOffset>>> mapped_chunk_offsets)
        : _chunk_id{chunk_id}, _matches_out{matches_out}, _mapped_chunk_offsets{std::move(mapped_chunk_offsets)} {}

    const ChunkID _chunk_id;
    PosList &_matches_out;

    std::unique_ptr<std::vector<std::pair<ChunkOffset, ChunkOffset>>> _mapped_chunk_offsets;
  };

 public:
  SingleColumnScanBase(std::shared_ptr<const Table> in_table, const ColumnID left_column_id, const ScanType scan_type,
                       const bool skip_null_row_ids = true)
      : ColumnScanBase{in_table, left_column_id, scan_type}, _skip_null_row_ids{skip_null_row_ids} {}

  PosList scan_chunk(const ChunkID &chunk_id) override {
    const auto &chunk = _in_table->get_chunk(chunk_id);
    const auto left_column = chunk.get_column(_left_column_id);

    auto matches_out = PosList{};
    auto context = std::make_shared<Context>(chunk_id, matches_out);

    left_column->visit(*this, context);

    return std::move(context->_matches_out);
  }

  void handle_reference_column(ReferenceColumn &left_column,
                               std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<Context>(base_context);
    const ChunkID chunk_id = context->_chunk_id;
    auto &matches_out = context->_matches_out;

    // TODO(mjendruk): Find a good estimates for when it’s better simply iterate over the column

    auto chunk_offsets_by_chunk_id = _split_position_list_by_chunk_id(*left_column.pos_list());

    // Visit each referenced column
    for (auto &pair : chunk_offsets_by_chunk_id) {
      const auto &referenced_chunk_id = pair.first;
      auto &mapped_chunk_offsets = pair.second;

      const auto &chunk = left_column.referenced_table()->get_chunk(referenced_chunk_id);
      auto referenced_column = chunk.get_column(left_column.referenced_column_id());

      auto mapped_chunk_offsets_ptr =
          std::make_unique<std::vector<std::pair<ChunkOffset, ChunkOffset>>>(std::move(mapped_chunk_offsets));

      auto new_context = std::make_shared<Context>(chunk_id, matches_out, std::move(mapped_chunk_offsets_ptr));
      referenced_column->visit(*this, new_context);
    }
  }

  /**
   * From ColumnVisitable:
   * virtual void handle_value_column(BaseColumn &base_column,
   *                                  std::shared_ptr<ColumnVisitableContext> base_context) = 0;
   *
   * virtual void handle_dictionary_column(BaseColumn &base_column,
   *                                       std::shared_ptr<ColumnVisitableContext> base_context) = 0;
   */

 private:
  /**
   * @defgroup Methods used for handling reference columns
   * @{
   */

  using ChunkOffsetsByChunkID = std::unordered_map<ChunkID, ChunkOffsetsList>;

  ChunkOffsetsByChunkID _split_position_list_by_chunk_id(const PosList &pos_list) {
    auto chunk_offsets_by_chunk_id = ChunkOffsetsByChunkID{};

    for (auto chunk_offset = 0u; chunk_offset < pos_list.size(); ++chunk_offset) {
      const auto row_id = pos_list[chunk_offset];

      if (_skip_null_row_ids && row_id == NULL_ROW_ID) continue;

      auto &mapped_chunk_offsets = chunk_offsets_by_chunk_id[row_id.chunk_id];
      mapped_chunk_offsets.emplace_back(chunk_offset, row_id.chunk_offset);
    }

    return chunk_offsets_by_chunk_id;
  }

  /**@}*/
 private:
  const bool _skip_null_row_ids;
};

/**
 * @brief Compares one column to a constant value
 *
 * - Value columns are scanned sequentially
 * - For dictionary columns, we basically look up the value ID of the constant value in the dictionary
 *   in order to avoid having to look up each value ID of the attribute vector in the dictionary. This also
 *   enables us to detect if all or none of the values in the column satisfy the expression.
 */
class SingleColumnScan : public SingleColumnScanBase {
 public:
  SingleColumnScan(std::shared_ptr<const Table> in_table, const ColumnID left_column_id, const ScanType &scan_type,
                   const AllTypeVariant &right_value)
      : SingleColumnScanBase{in_table, left_column_id, scan_type}, _right_value{right_value} {}

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<Context>(base_context);

    const auto left_column_type = _in_table->column_type(_left_column_id);

    resolve_type(left_column_type, [&](auto type) {
      using Type = typename decltype(type)::type;

      auto &left_column = static_cast<ValueColumn<Type> &>(base_column);

      auto left_column_iterable = _create_iterable_from_column(left_column, context->_mapped_chunk_offsets.get());
      auto right_value_iterable = ConstantValueIterable<Type>{_right_value};

      left_column_iterable.get_iterators([&](auto left_it, auto left_end) {
        right_value_iterable.get_iterators([&](auto right_it, auto right_end) {
          resolve_operator_type(_scan_type, [&](auto comparator) {
            Scan{context->_chunk_id, context->_matches_out}(comparator, left_it, left_end, right_it);  // NOLINT
          });
        });
      });
    });
  }

  void handle_dictionary_column(BaseColumn &base_column,
                                std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<Context>(base_context);
    auto &matches_out = context->_matches_out;
    const auto chunk_id = context->_chunk_id;
    auto &left_column = static_cast<const UntypedDictionaryColumn &>(base_column);

    // TODO(mjendruk): Find a good heuristic for when simply scanning column is faster (dictionary size -> attribute
    // vector size)

    /**
     * ValueID value_id; // left value id
     * Variant value; // right value
     *
     * A ValueID value_id from the attribute vector is included in the result iff
     *
     * Operator           |  Condition
     * value_id == value  |  dict.value_by_value_id(dict.lower_bound(value)) == value && value_id == dict.lower_bound(value)
     * value_id != value  |  dict.value_by_value_id(dict.lower_bound(value)) != value || value_id != dict.lower_bound(value)
     * value_id <  value  |  value_id < dict.lower_bound(value)
     * value_id <= value  |  value_id < dict.upper_bound(value)
     * value_id >  value  |  value_id >= dict.upper_bound(value)
     * value_id >= value  |  value_id >= dict.lower_bound(value)
     */

    const auto search_value_id = _get_search_value_id(left_column);

    /**
     * Early Outs
     *
     * Operator          | All                                   | None
     * value_id == value | !None && unique_values_count == 1     | search_vid == dict.upper_bound(value)
     * value_id != value | search_vid == dict.upper_bound(value) | !All && unique_values_count == 1
     * value_id <  value | search_vid == INVALID_VALUE_ID        | search_vid == 0
     * value_id <= value | search_vid == INVALID_VALUE_ID        | search_vid == 0
     * value_id >  value | search_vid == 0                       | search_vid == INVALID_VALUE_ID
     * value_id >= value | search_vid == 0                       | search_vid == INVALID_VALUE_ID
     */

    const auto &attribute_vector = *left_column.attribute_vector();
    auto attribute_vector_iterable = AttributeVectorIterable{attribute_vector, context->_mapped_chunk_offsets.get()};

    if (_right_value_matches_all(left_column, search_value_id)) {
      attribute_vector_iterable.get_iterators([&](auto left_it, auto left_end) {
        for (; left_it != left_end; ++left_it) {
          const auto left = *left_it;

          if (left.is_null()) continue;
          matches_out.push_back(RowID{context->_chunk_id, left.chunk_offset()});
        }
      });

      return;
    }

    if (_right_value_matches_none(left_column, search_value_id)) {
      return;
    }

    auto constant_value_iterable = ConstantValueIterable<ValueID>{search_value_id};

    attribute_vector_iterable.get_iterators([&](auto left_it, auto left_end) {
      constant_value_iterable.get_iterators([&](auto right_it, auto right_end) {
        this->_resolve_scan_type([&](auto comparator) {
          Scan{chunk_id, matches_out}(comparator, left_it, left_end, right_it);  // NOLINT
        });
      });
    });
  }

 private:
  /**
   * @defgroup Methods used for handling value columns
   * @{
   */

  template <typename Type>
  static auto _create_iterable_from_column(
      ValueColumn<Type> &column, const std::vector<std::pair<ChunkOffset, ChunkOffset>> *mapped_chunk_offsets) {
    return ValueColumnIterable<Type>{column, mapped_chunk_offsets};
  }

  /**@}*/

 private:
  /**
   * @defgroup Methods used for handling dictionary columns
   * @{
   */

  ValueID _get_search_value_id(const UntypedDictionaryColumn &column) {
    switch (_scan_type) {
      case ScanType::OpEquals:
      case ScanType::OpNotEquals:
      case ScanType::OpLessThan:
      case ScanType::OpGreaterThanEquals:
        return column.lower_bound(_right_value);

      case ScanType::OpLessThanEquals:
      case ScanType::OpGreaterThan:
        return column.upper_bound(_right_value);

      default:
        Fail("Unsupported comparison type encountered");
        return INVALID_VALUE_ID;
    }
  }

  bool _right_value_matches_all(const UntypedDictionaryColumn &column, const ValueID search_value_id) {
    switch (_scan_type) {
      case ScanType::OpEquals:
        return search_value_id != column.upper_bound(_right_value) && column.unique_values_count() == size_t{1u};

      case ScanType::OpNotEquals:
        return search_value_id == column.upper_bound(_right_value);

      case ScanType::OpLessThan:
      case ScanType::OpLessThanEquals:
        return search_value_id == INVALID_VALUE_ID;

      case ScanType::OpGreaterThanEquals:
      case ScanType::OpGreaterThan:
        return search_value_id == ValueID{0u};

      default:
        Fail("Unsupported comparison type encountered");
        return false;
    }
  }

  bool _right_value_matches_none(const UntypedDictionaryColumn &column, const ValueID search_value_id) {
    switch (_scan_type) {
      case ScanType::OpEquals:
        return search_value_id == column.upper_bound(_right_value);

      case ScanType::OpNotEquals:
        return search_value_id == column.upper_bound(_right_value) && column.unique_values_count() == size_t{1u};

      case ScanType::OpLessThan:
      case ScanType::OpLessThanEquals:
        return search_value_id == ValueID{0u};

      case ScanType::OpGreaterThan:
      case ScanType::OpGreaterThanEquals:
        return search_value_id == INVALID_VALUE_ID;

      default:
        Fail("Unsupported comparison type encountered");
        return false;
    }
  }

  template <typename Functor>
  void _resolve_scan_type(const Functor &func) {
    switch (_scan_type) {
      case ScanType::OpEquals:
        func(std::equal_to<void>{});
        return;

      case ScanType::OpNotEquals:
        func(std::not_equal_to<void>{});
        return;

      case ScanType::OpLessThan:
      case ScanType::OpLessThanEquals:
        func(std::less<void>{});
        return;

      case ScanType::OpGreaterThan:
      case ScanType::OpGreaterThanEquals:
        func(std::greater_equal<void>{});
        return;

      default:
        Fail("Unsupported comparison type encountered");
        return;
    }
  }

  /**@}*/

 private:
  const AllTypeVariant _right_value;
};

/**
 * @brief Scans any column sequentially
 *
 * This implementation of single column scan does not use any optimizations and
 * scan any column sequentially.
 */
/*
class SimpleSingleColumnScan : public ColumnScanBase {
 public:
  SimpleSingleColumnScan(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                         const ScanType & scan_type, const AllTypeVariant & right_value)
      : ColumnScanBase{in_table, left_column_id, scan_type}, _right_value{right_value} {}

  PosList scan_chunk(const ChunkID & chunk_id) override {
    const auto & chunk = _in_table->get_chunk(chunk_id);

    const auto left_column_type = _in_table->column_type(_left_column_id);
    const auto left_column = chunk.get_column(_left_column_id);

    auto matches_out = PosList{};

    resolve_column_type(left_column_type, *left_column, [&](auto type, auto &typed_left_column) {
      using Type = typename decltype(type)::type;

      auto left_column_iterable = _create_iterable_from_column<Type>(typed_left_column);
      auto right_value_iterable = ConstantValueIterable<Type>{_right_value};

      left_column_iterable.get_iterators_no_indices([&](auto left_it, auto left_end) {
        right_value_iterable.get_iterators_no_indices([&](auto right_it, auto right_end) {
          resolve_operator_type(_scan_type, [&](auto comparator) {
            Scan{chunk_id, matches_out}(comparator, left_it, left_end, right_it);
          });
        });
      });
    });

    return matches_out;
  }

 private:
  const AllTypeVariant _right_value;
};
*/

class NullColumnScan : public SingleColumnScanBase {
 public:
  NullColumnScan(std::shared_ptr<const Table> in_table, const ColumnID left_column_id, const ScanType &scan_type)
      : SingleColumnScanBase{in_table, left_column_id, scan_type, false} {}

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<Context>(base_context);
    auto &left_column = static_cast<const UntypedValueColumn &>(base_column);

    if (_matches_all(left_column)) {
      _add_all(*context, left_column.size());
      return;
    }

    if (_matches_none(left_column)) {
      return;
    }

    DebugAssert(left_column.is_nullable(),
                "Columns that are not nullable should have been caught by edge case handling.");

    auto left_column_iterable =
        NullValueVectorIterable{left_column.null_values(), context->_mapped_chunk_offsets.get()};

    left_column_iterable.get_iterators([&](auto left_it, auto left_end) { this->_scan(left_it, left_end, *context); });
  }

  void handle_dictionary_column(BaseColumn &base_column,
                                std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<Context>(base_context);
    auto &left_column = static_cast<const UntypedDictionaryColumn &>(base_column);

    auto left_column_iterable =
        AttributeVectorIterable{*left_column.attribute_vector(), context->_mapped_chunk_offsets.get()};

    left_column_iterable.get_iterators([&](auto left_it, auto left_end) { this->_scan(left_it, left_end, *context); });
  }

 private:
  /**
   * @defgroup Methods used for handling value columns
   * @{
   */

  bool _matches_all(const UntypedValueColumn &column) {
    switch (_scan_type) {
      case ScanType::OpEquals:
        return false;

      case ScanType::OpNotEquals:
        return !column.is_nullable();

      default:
        Fail("Unsupported comparison type encountered");
        return false;
    }
  }

  bool _matches_none(const UntypedValueColumn &column) {
    switch (_scan_type) {
      case ScanType::OpEquals:
        return !column.is_nullable();

      case ScanType::OpNotEquals:
        return false;

      default:
        Fail("Unsupported comparison type encountered");
        return false;
    }
  }

  void _add_all(Context &context, size_t column_size) {
    auto &matches_out = context._matches_out;
    const auto chunk_id = context._chunk_id;
    const auto &mapped_chunk_offsets = context._mapped_chunk_offsets;

    if (mapped_chunk_offsets) {
      for (const auto &chunk_offsets : *mapped_chunk_offsets) {
        matches_out.push_back(RowID{chunk_id, chunk_offsets.first});
      }
    } else {
      for (auto chunk_offset = 0u; chunk_offset < column_size; ++chunk_offset) {
        matches_out.push_back(RowID{chunk_id, chunk_offset});
      }
    }
  }

  /**@}*/

 private:
  template <typename Functor>
  void _resolve_scan_type(const Functor &func) {
    switch (_scan_type) {
      case ScanType::OpEquals:
        return func([](const bool is_null) { return is_null; });

      case ScanType::OpNotEquals:
        return func([](const bool is_null) { return !is_null; });

      default:
        Fail("Unsupported comparison type encountered");
    }
  }

  template <typename Iterator>
  void _scan(Iterator left_it, Iterator left_end, Context &context) {
    auto &matches_out = context._matches_out;
    const auto chunk_id = context._chunk_id;

    _resolve_scan_type([&](auto comparator) {
      for (; left_it != left_end; ++left_it) {
        const auto left = *left_it;

        if (!comparator(left.is_null())) continue;
        matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
      }
    });
  }
};

/**
 * @brief Implements a column scan using the LIKE operator
 *
 * - The only supported type is std::string.
 * - Value columns are scanned sequentially
 * - For dictionary columns, we check the values in the dictionary and store the results in a vector
 *   in order to avoid having to look up each value ID of the attribute vector in the dictionary. This also
 *   enables us to detect if all or none of the values in the column satisfy the expression.
 */
class LikeColumnScan : public SingleColumnScanBase {
 public:
  LikeColumnScan(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                 const std::string &right_wildcard)
      : SingleColumnScanBase{in_table, left_column_id, ScanType::OpLike}, _right_wildcard{right_wildcard} {
    // convert the given SQL-like search term into a c++11 regex to use it for the actual matching
    auto regex_string = _sqllike_to_regex(_right_wildcard);
    _regex = std::regex{regex_string, std::regex_constants::icase};  // case insentivity
  }

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<Context>(base_context);
    auto &matches_out = context->_matches_out;
    const auto chunk_id = context->_chunk_id;

    auto &left_column = static_cast<const ValueColumn<std::string> &>(base_column);

    auto left_column_iterable = ValueColumnIterable<std::string>{left_column, context->_mapped_chunk_offsets.get()};

    left_column_iterable.get_iterators([&](auto left_it, auto left_end) {
      for (; left_it != left_end; ++left_it) {
        const auto left = *left_it;

        if (left.is_null()) continue;

        if (std::regex_match(left.value(), _regex)) {
          matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
        }
      }
    });
  }

  void handle_dictionary_column(BaseColumn &base_column,
                                std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<Context>(base_context);
    auto &matches_out = context->_matches_out;
    const auto chunk_id = context->_chunk_id;

    const auto &left_column = static_cast<const DictionaryColumn<std::string> &>(base_column);

    // TODO(mjendruk): Find a good heuristic for when simply scanning column is faster (dictionary size -> attribute
    // vector size)

    const auto dictionary_matches = _find_matches_in_dictionary(*left_column.dictionary());

    const auto match_count =
        static_cast<size_t>(std::count(dictionary_matches.cbegin(), dictionary_matches.cend(), true));

    const auto &attribute_vector = *left_column.attribute_vector();
    auto attribute_vector_iterable = AttributeVectorIterable{attribute_vector, context->_mapped_chunk_offsets.get()};

    if (match_count == dictionary_matches.size()) {
      attribute_vector_iterable.get_iterators([&](auto left_it, auto left_end) {
        for (; left_it != left_end; ++left_it) {
          const auto left = *left_it;

          if (left.is_null()) continue;

          matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
        }
      });
    }

    if (match_count == 0u) {
      return;
    }

    attribute_vector_iterable.get_iterators([&](auto left_it, auto left_end) {
      for (; left_it != left_end; ++left_it) {
        const auto left = *left_it;

        if (left.is_null()) continue;

        if (dictionary_matches[left.value()]) {
          matches_out.push_back(RowID{chunk_id, left.chunk_offset()});
        }
      }
    });
  }

 private:
  /**
   * @defgroup Methods used for handling dictionary columns
   * @{
   */

  std::vector<bool> _find_matches_in_dictionary(const std::vector<std::string> &dictionary) {
    auto dictionary_matches = std::vector<bool>{};
    dictionary_matches.reserve(dictionary.size());

    for (const auto &value : dictionary) {
      dictionary_matches.push_back(std::regex_match(value, _regex));
    }

    return dictionary_matches;
  }

  /**@}*/

 private:
  /**
   * @defgroup Methods which are used to convert an SQL wildcard into a C++ regex.
   * @{
   */

  static std::string &_replace_all(std::string &str, const std::string &old_value, const std::string &new_value) {
    std::string::size_type pos = 0;
    while ((pos = str.find(old_value, pos)) != std::string::npos) {
      str.replace(pos, old_value.size(), new_value);
      pos += new_value.size() - old_value.size() + 1;
    }
    return str;
  }

  static std::map<std::string, std::string> _extract_character_ranges(std::string &str) {
    std::map<std::string, std::string> ranges;

    int rangeID = 0;
    std::string::size_type startPos = 0;
    std::string::size_type endPos = 0;

    while ((startPos = str.find("[", startPos)) != std::string::npos &&
           (endPos = str.find("]", startPos + 1)) != std::string::npos) {
      std::stringstream ss;
      ss << "[[" << rangeID << "]]";
      std::string chars = str.substr(startPos + 1, endPos - startPos - 1);
      str.replace(startPos, chars.size() + 2, ss.str());
      rangeID++;
      startPos += ss.str().size();

      _replace_all(chars, "[", "\\[");
      _replace_all(chars, "]", "\\]");
      ranges[ss.str()] = "[" + chars + "]";
    }

    int open = 0;
    std::string::size_type searchPos = 0;
    startPos = 0;
    endPos = 0;
    do {
      startPos = str.find("[", searchPos);
      endPos = str.find("]", searchPos);

      if (startPos == std::string::npos && endPos == std::string::npos) break;

      if (startPos < endPos || endPos == std::string::npos) {
        open++;
        searchPos = startPos + 1;
      } else {
        if (open <= 0) {
          str.replace(endPos, 1, "\\]");
          searchPos = endPos + 2;
        } else {
          open--;
          searchPos = endPos + 1;
        }
      }
    } while (searchPos < str.size());
    return ranges;
  }

  static std::string _sqllike_to_regex(std::string sqllike) {
    _replace_all(sqllike, ".", "\\.");
    _replace_all(sqllike, "^", "\\^");
    _replace_all(sqllike, "$", "\\$");
    _replace_all(sqllike, "+", "\\+");
    _replace_all(sqllike, "?", "\\?");
    _replace_all(sqllike, "(", "\\(");
    _replace_all(sqllike, ")", "\\)");
    _replace_all(sqllike, "{", "\\{");
    _replace_all(sqllike, "}", "\\}");
    _replace_all(sqllike, "\\", "\\\\");
    _replace_all(sqllike, "|", "\\|");
    _replace_all(sqllike, ".", "\\.");
    _replace_all(sqllike, "*", "\\*");
    std::map<std::string, std::string> ranges = _extract_character_ranges(sqllike);  // Escapes [ and ] where necessary
    _replace_all(sqllike, "%", ".*");
    _replace_all(sqllike, "_", ".");
    for (auto &range : ranges) {
      _replace_all(sqllike, range.first, range.second);
    }
    return "^" + sqllike + "$";
  }

  /**@}*/

 private:
  const std::string _right_wildcard;

  std::regex _regex;
};

/**
 * @brief Compares two columns to each other
 *
 * Does not support:
 * - comparison of string columns to numerical columns
 * - reference columns to data columns (value, dictionary)
 */
class ColumnComparisonScan : public ColumnScanBase {
 public:
  ColumnComparisonScan(std::shared_ptr<const Table> in_table, const ColumnID left_column_id, const ScanType &scan_type,
                       const ColumnID right_column_id)
      : ColumnScanBase{in_table, left_column_id, scan_type}, _right_column_id{right_column_id} {}

  PosList scan_chunk(const ChunkID &chunk_id) override {
    const auto &chunk = _in_table->get_chunk(chunk_id);
    const auto left_column_type = _in_table->column_type(_left_column_id);
    const auto right_column_type = _in_table->column_type(_right_column_id);

    const auto left_column = chunk.get_column(_left_column_id);
    const auto right_column = chunk.get_column(_right_column_id);

    auto matches_out = PosList{};

    resolve_column_type(left_column_type, *left_column, [&](auto left_type, auto &typed_left_column) {
      resolve_column_type(right_column_type, *right_column, [&](auto right_type, auto &typed_right_column) {
        using LeftColumnType = typename std::decay<decltype(typed_left_column)>::type;
        using RightColumnType = typename std::decay<decltype(typed_right_column)>::type;

        using LeftType = typename decltype(left_type)::type;
        using RightType = typename decltype(right_type)::type;

        /**
         * This generic lambda is instantiated for each type (int, long, etc.) and
         * each column type (value, dictionary, reference column) per column!
         * That’s 3x5 combinations each and 15x15=225 in total. However, not all combinations are valid or possible.
         * Only data columns (value, dictionary) or reference columns will be compared, as a table with both data and
         * reference columns is ruled out. Moreover it is not possible to compare strings to any of the four numerical
         * data types. Therefore, we need to check for these cases and exclude them via the constexpr-if which
         * reduces the number of combinations to 85.
         */

        constexpr auto left_is_reference_column = (std::is_same<LeftColumnType, ReferenceColumn>{});
        constexpr auto right_is_reference_column = (std::is_same<RightColumnType, ReferenceColumn>{});

        constexpr auto neither_is_reference_column = !left_is_reference_column && !right_is_reference_column;
        constexpr auto both_are_reference_columns = left_is_reference_column && right_is_reference_column;

        constexpr auto left_is_string_column = (std::is_same<LeftType, std::string>{});
        constexpr auto right_is_string_column = (std::is_same<RightType, std::string>{});

        constexpr auto neither_is_string_column = !left_is_string_column && !right_is_string_column;
        constexpr auto both_are_string_columns = left_is_string_column && right_is_string_column;

        static_if<(neither_is_reference_column || both_are_reference_columns) &&
                  (neither_is_string_column || both_are_string_columns)>([&](auto f) {
          auto left_column_iterable = _create_iterable_from_column<LeftType>(typed_left_column);
          auto right_column_iterable = _create_iterable_from_column<RightType>(typed_right_column);

          left_column_iterable.get_iterators_no_indices([&](auto left_it, auto left_end) {
            right_column_iterable.get_iterators_no_indices([&](auto right_it, auto right_end) {
              resolve_operator_type(_scan_type, [&](auto comparator) {
                Scan{chunk_id, matches_out}(comparator, left_it, left_end, right_it);  // NOLINT
              });
            });
          });
        }).else_([&](auto f) {
          Fail("Invalid column combination detected!");
        });
      });
    });

    return matches_out;
  }

 private:
  const ColumnID _right_column_id;
};

TableScan::TableScan(const std::shared_ptr<AbstractOperator> in, const std::string &left_column_name,
                     const ScanType scan_type, const AllParameterVariant right_parameter,
                     const optional<AllTypeVariant> right_value2)
    : AbstractReadOnlyOperator{in},
      _left_column_name{left_column_name},
      _scan_type{scan_type},
      _right_parameter{right_parameter},
      _right_value2{right_value2} {}

TableScan::~TableScan() = default;

const std::string TableScan::name() const { return "TableScan"; }

uint8_t TableScan::num_in_tables() const { return 1; }

uint8_t TableScan::num_out_tables() const { return 1; }

std::shared_ptr<AbstractOperator> TableScan::recreate(const std::vector<AllParameterVariant> &args) const {
  // Replace value in the new operator, if it’s a parameter and an argument is available.
  if (is_placeholder(_right_parameter)) {
    const auto index = boost::get<ValuePlaceholder>(_right_parameter).index();
    if (index < args.size()) {
      return std::make_shared<TableScan>(_input_left->recreate(args), _left_column_name, _scan_type, args[index],
                                         _right_value2);
    }
  }
  return std::make_shared<TableScan>(_input_left->recreate(args), _left_column_name, _scan_type, _right_parameter,
                                     _right_value2);
}

std::shared_ptr<const Table> TableScan::on_execute() {
  _in_table = input_table_left();

  init_scan();
  init_output_table();

  std::mutex output_mutex;

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(_in_table->chunk_count());

  for (ChunkID chunk_id{0u}; chunk_id < _in_table->chunk_count(); ++chunk_id) {
    auto job_task = std::make_shared<JobTask>([=, &output_mutex]() {
      // The actual scan happens in the sub classes of ColumnScanBase
      const auto matches_out = std::make_shared<PosList>(_scan->scan_chunk(chunk_id));

      Chunk chunk_out;

      /**
       * matches_out contains a list of row IDs into this chunk. If this is not a reference table, we can
       * directly use the matches to construct the reference columns of the output. If it is a reference column,
       * we need to resolve the row IDs so that they reference the physical data columns (value, dictionary) instead,
       * since we don’t allow multi-level referencing. To save time and space, we want to share positions lists
       * between columns as much as possible. Position lists can be shared between two columns iff
       * (a) they point to the same table and
       * (b) the reference columns of the input table point to the same positions in the same order
       *     (i.e. they share their position list).
       */
      if (_is_reference_table) {
        const auto &chunk_in = _in_table->get_chunk(chunk_id);

        auto filtered_pos_lists = std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>>{};

        // TODO(mjendruk): implement shortcut for pos_list is the same as the scanned column’s

        for (ColumnID column_id{0u}; column_id < _in_table->col_count(); ++column_id) {
          auto column_in = chunk_in.get_column(column_id);

          auto ref_column_in = std::dynamic_pointer_cast<const ReferenceColumn>(column_in);
          DebugAssert(ref_column_in != nullptr, "All columns should be of type ReferenceColumn.");

          const auto pos_list_in = ref_column_in->pos_list();

          const auto table_out = ref_column_in->referenced_table();
          const auto column_id_out = ref_column_in->referenced_column_id();

          auto &filtered_pos_list = filtered_pos_lists[pos_list_in];

          if (!filtered_pos_list) {
            filtered_pos_list = std::make_shared<PosList>();
            filtered_pos_list->reserve(matches_out->size());

            for (const auto &match : *matches_out) {
              const auto row_id = (*pos_list_in)[match.chunk_offset];
              filtered_pos_list->push_back(row_id);
            }
          }

          auto ref_column_out = std::make_shared<ReferenceColumn>(table_out, column_id_out, filtered_pos_list);
          chunk_out.add_column(ref_column_out);
        }
      } else {
        for (ColumnID column_id{0u}; column_id < _in_table->col_count(); ++column_id) {
          auto ref_column_out = std::make_shared<ReferenceColumn>(_in_table, column_id, matches_out);
          chunk_out.add_column(ref_column_out);
        }
      }

      std::lock_guard<std::mutex> lock(output_mutex);
      _output_table->add_chunk(std::move(chunk_out));
    });

    jobs.push_back(job_task);
    job_task->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);

  return _output_table;
}

void TableScan::init_scan() {
  const auto left_column_id = _in_table->column_id_by_name(_left_column_name);

  DebugAssert(_in_table->chunk_count() > 0u, "Input table must contain at least 1 chunk.");
  const auto &first_chunk = _in_table->get_chunk(ChunkID{0u});

  _is_reference_table = first_chunk.get_column(left_column_id)->is_reference_column();

  if (_scan_type == ScanType::OpLike) {
    const auto left_column_type = _in_table->column_type(left_column_id);
    DebugAssert((left_column_type == "string"), "LIKE operator only applicable on string columns.");

    /**
     * LIKE is always executed on with constant value (containing a wildcard)
     * using a VariableTerm (ColumnName, see term.hpp) is not supported here
     */
    DebugAssert(is_variant(_right_parameter), "LIKE only supports ConstantTerms.");

    const auto right_value = boost::get<AllTypeVariant>(_right_parameter);

    DebugAssert(!is_null(right_value), "Right value must not be NULL.");

    const auto right_wildcard = type_cast<std::string>(right_value);

    _scan = std::make_unique<LikeColumnScan>(_in_table, left_column_id, right_wildcard);

    return;
  }

  if (is_variant(_right_parameter)) {
    const auto right_value = boost::get<AllTypeVariant>(_right_parameter);

    if (is_null(right_value)) {
      _scan = std::make_unique<NullColumnScan>(_in_table, left_column_id, _scan_type);

      return;
    }

    DebugAssert(!is_null(right_value), "Right value must not be NULL.");

    _scan = std::make_unique<SingleColumnScan>(_in_table, left_column_id, _scan_type, right_value);

    // _scan = std::make_unique<SimpleSingleColumnScan>(_in_table, left_column_id, _scan_type, right_value);
  } else /* is_column_name(_right_parameter) */ {
    const auto right_column_name = boost::get<ColumnName>(_right_parameter);
    const auto right_column_id = _in_table->column_id_by_name(right_column_name);

    _scan = std::make_unique<ColumnComparisonScan>(_in_table, left_column_id, _scan_type, right_column_id);
  }
}

void TableScan::init_output_table() {
  _output_table = std::make_shared<Table>();

  for (ColumnID column_id{0}; column_id < _in_table->col_count(); ++column_id) {
    _output_table->add_column_definition(_in_table->column_name(column_id), _in_table->column_type(column_id));
  }
}

}  // namespace opossum
