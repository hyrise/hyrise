#pragma once

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <regex>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

#include "all_parameter_variant.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

// operator to filter a table by a single attribute
// output is an table with only reference columns
// to filter by multiple criteria, you can chain the operator

// As with most operators, we do not guarantee a stable operation with regards to positions - i.e., your sorting order
// might be disturbed

// Because the BETWEEN operator needs 3 values, we didn't implemented the possibilty to scan on 3 columns, as this
// would increase the if-complexity in handle_column again.
// That's why we only accept a constant value for the second between-parameter.

class TableScan : public AbstractReadOnlyOperator {
 public:
  TableScan(const std::shared_ptr<AbstractOperator> in, const std::string &filter_column_name, const std::string &op,
            const AllParameterVariant value, const optional<AllTypeVariant> value2 = nullopt);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  std::shared_ptr<const Table> on_execute() override;

  template <typename T>
  class TableScanImpl;

  const std::string _column_name;
  const std::string _op;
  const AllParameterVariant _value;
  const optional<AllTypeVariant> _value2;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;

  enum ScanType {
    OpEquals,
    OpNotEquals,
    OpLessThan,
    OpLessThanEquals,
    OpGreaterThan,
    OpGreaterThanEquals,
    OpBetween,
    OpLike
  };

  static std::string &replace_all(std::string &str, const std::string &old_value, const std::string &new_value);
  static std::map<std::string, std::string> extract_character_ranges(std::string &str);
  static std::string sqllike_to_regex(std::string sqllike);
};

// we need to use the impl pattern because the scan operator of the sort depends on the type of the column
template <typename T>
class TableScan::TableScanImpl : public AbstractReadOnlyOperatorImpl {
 public:
  // supported values for op are {"=", "!=", "<", "<=", ">", ">=", "BETWEEN"}
  // creates a new table with reference columns
  TableScanImpl(const std::shared_ptr<const AbstractOperator> in, const std::string &filter_column_name,
                const std::string &op, const AllParameterVariant value, const optional<AllTypeVariant> value2)
      : _in_operator(in),
        _filter_column_name(filter_column_name),
        _op(op),
        _value(value),
        _value2(value2),
        _is_constant_value_scan(_value.type() == typeid(AllTypeVariant)) {}

  struct ScanContext : ColumnVisitableContext {
    ScanContext(std::shared_ptr<const Table> t, ChunkID c, std::vector<RowID> &mo,
                const tbb::concurrent_vector<T> &values, std::shared_ptr<std::vector<ChunkOffset>> co = nullptr)
        : table_in(t), chunk_id(c), matches_out(mo), values(values), chunk_offsets_in(std::move(co)) {}

    // constructor for use in ReferenceColumn::visit_dereferenced
    ScanContext(std::shared_ptr<BaseColumn>, const std::shared_ptr<const Table> referenced_table,
                std::shared_ptr<ColumnVisitableContext> base_context, ChunkID chunk_id,
                std::shared_ptr<std::vector<ChunkOffset>> chunk_offsets)
        : table_in(referenced_table),
          chunk_id(chunk_id),
          matches_out(std::static_pointer_cast<ScanContext>(base_context)->matches_out),
          values(std::static_pointer_cast<ScanContext>(base_context)->values),
          chunk_offsets_in(chunk_offsets) {}

    std::shared_ptr<const Table> table_in;
    const ChunkID chunk_id;
    std::vector<RowID> &matches_out;
    const tbb::concurrent_vector<T> &values;
    std::shared_ptr<std::vector<ChunkOffset>> chunk_offsets_in;
  };

  std::shared_ptr<const Table> on_execute() override {
    auto output = std::make_shared<Table>();

    auto in_table = _in_operator->get_output();
    ColumnID column_id1, column_id2;
    T casted_value1;
    optional<T> casted_value2;
    std::string like_regex;

    column_id1 = in_table->column_id_by_name(_filter_column_name);
    if (_is_constant_value_scan) {
      // column_a == 5
      casted_value1 = type_cast<T>(boost::get<AllTypeVariant>(_value));
      if (_value2) casted_value2 = boost::get<T>(*_value2);
    } else {
      // column_a == column_b
      ColumnName column_name = boost::get<ColumnName>(_value);
      column_id2 = in_table->column_id_by_name(column_name);
    }

    for (size_t column_id = 0; column_id < in_table->col_count(); ++column_id) {
      output->add_column(in_table->column_name(column_id), in_table->column_type(column_id), false);
    }

    // Definining all possible operators here might appear odd. Chances are, however, that we will not
    // have a similar comparison anywhere else. Index scans, for example, would not use an adaptable binary
    // predicate, but will have to use different methods (lower_range, upper_range, ...) based on the
    // chosen operator. For now, we can save us some dark template magic by using the switch below.
    // DO NOT copy this code, however, without discussing if there is a better way to avoid code duplication.

    if (_op == "=") {
      _type = OpEquals;
      _value_comparator = [](T left, T right) { return left == right; };
      _value_id_comparator = [](ValueID found_vid, ValueID search_vid, ValueID) { return found_vid == search_vid; };
    } else if (_op == "!=") {
      _type = OpNotEquals;
      _value_comparator = [](T left, T right) { return left != right; };
      _value_id_comparator = [](ValueID found_vid, ValueID search_vid, ValueID) { return found_vid != search_vid; };
    } else if (_op == "<") {
      _type = OpLessThan;
      _value_comparator = [](T left, T right) { return left < right; };
      _value_id_comparator = [](ValueID found_vid, ValueID search_vid, ValueID) { return found_vid < search_vid; };
    } else if (_op == "<=") {
      _type = OpLessThanEquals;
      _value_comparator = [](T left, T right) { return left <= right; };
      _value_id_comparator = [](ValueID found_vid, ValueID search_vid, ValueID) { return found_vid < search_vid; };
      //                                                                                           ^
      //                                                               sic! see handle_dictionary_column for details
    } else if (_op == ">") {
      _type = OpGreaterThan;
      _value_comparator = [](T left, T right) { return left > right; };
      _value_id_comparator = [](ValueID found_vid, ValueID search_vid, ValueID) { return found_vid >= search_vid; };
    } else if (_op == ">=") {
      _type = OpGreaterThanEquals;
      _value_comparator = [](T left, T right) { return left >= right; };
      _value_id_comparator = [](ValueID found_vid, ValueID search_vid, ValueID) { return found_vid >= search_vid; };
    } else if (_op == "BETWEEN") {
      DebugAssert(static_cast<bool>(casted_value2), "No second value for BETWEEN comparison given");

      _type = OpBetween;
      _value_comparator = [casted_value2](T value, T left) { return value >= left && value <= casted_value2; };
      _value_id_comparator = [](ValueID found_vid, ValueID search_vid, ValueID search_vid2) {
        return search_vid <= found_vid && found_vid < search_vid2;
      };
    } else if (_op == "LIKE") {
      // LIKE is always executed on with constant value (containing a wildcard)
      // using VariableTerm is not supported here
      DebugAssert(_is_constant_value_scan, "LIKE only supports ConstantTerms and std::string type");
      const auto column_type = in_table->column_type(column_id1);
      DebugAssert((column_type == "string"), "LIKE operator only applicable on string columns");

      _type = OpLike;
    } else {
      DebugFail(std::string("unknown operator ") + _op);
    }

    // We can easily distribute the table scanning work on individual chunks to multiple sub tasks,
    // we just need to synchronize access to the output table
    std::mutex output_mutex;
    std::vector<std::shared_ptr<AbstractTask>> jobs;
    jobs.reserve(in_table->chunk_count());

    for (ChunkID chunk_id = 0; chunk_id < in_table->chunk_count(); ++chunk_id) {
      jobs.emplace_back(std::make_shared<JobTask>([&in_table, chunk_id, &output_mutex, &output, &column_id1,
                                                   &column_id2, &casted_value1, &casted_value2, this]() {
        const Chunk &chunk_in = in_table->get_chunk(chunk_id);
        Chunk chunk_out;

        std::vector<RowID> matches_in_this_chunk;
        auto column1 = chunk_in.get_column(column_id1);

        tbb::concurrent_vector<T> values;
        auto context = std::make_shared<ScanContext>(in_table, chunk_id, matches_in_this_chunk, values);

        // The real tablescan work happens now in the visitables. There are two major types of the Visitables: Column
        // and Constant.
        // Because Like can be optimized it has its own Visitable (constant only)

        if (_type == OpLike) {
          auto visitable = TableScanLikeVisitable(type_cast<std::string>(casted_value1));
          column1->visit(visitable, context);
        } else if (_is_constant_value_scan) {
          auto visitable = TableScanConstantColumnVisitable(_value_comparator, _value_id_comparator, _type,
                                                            casted_value1, casted_value2);
          column1->visit(visitable, context);
        } else {
          // the second column gets materialized
          auto column2 = chunk_in.get_column(column_id2);
          if (auto value_column = std::dynamic_pointer_cast<ValueColumn<T>>(column2)) {
            values = value_column->values();  // copy here is unneccesary, but shared pointer impl is needed
          } else if (auto dict_column = std::dynamic_pointer_cast<DictionaryColumn<T>>(column2)) {
            values = dict_column->materialize_values();
          } else if (auto ref_column = std::dynamic_pointer_cast<ReferenceColumn>(column2)) {
            values = ref_column->template materialize_values<T>();  // Clang needs the template prefix
          }
          auto visitable = TableScanVariableColumnVisitable(_value_comparator, _value_id_comparator);
          column1->visit(visitable, context);
        }

        // We now receive the visits in the handler methods below...

        // Even if we don't have any matches in this chunk, we need to correctly set the output columns.
        // If we would just return here, we would end up with a Chunk without Columns,
        // which makes the output unusable for further operations (-> OperatorsTableScanTest::ScanWithEmptyInput)

        // Ok, now we have a list of the matching positions relative to this chunk (ChunkOffsets). Next, we have to
        // transform them into absolute row ids. To save time and space, we want to share PosLists between columns
        // as much as possible. All ValueColumns and DictionaryColumns can share the same PosLists because they use
        // no
        // further  redirection. For ReferenceColumns, PosLists can be shared between two columns iff (a) they point
        // to the same table and (b) the incoming ReferenceColumns point to the same positions in the same order.
        // To make this check easier, we share PosLists between two ReferenceColumns iff they shared PosLists in
        // the incoming table as well. _filtered_pos_lists will hold a mapping from incoming PosList to outgoing
        // PosList. Because Value/DictionaryColumns do not have an incoming PosList, they are represented with
        // nullptr.
        std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>> filtered_pos_lists;
        for (size_t column_id = 0; column_id < in_table->col_count(); ++column_id) {
          auto ref_col_in = std::dynamic_pointer_cast<ReferenceColumn>(chunk_in.get_column(column_id));
          std::shared_ptr<const PosList> pos_list_in;
          std::shared_ptr<const Table> referenced_table_out;
          size_t referenced_column_id;
          if (ref_col_in) {
            pos_list_in = ref_col_in->pos_list();
            referenced_table_out = ref_col_in->referenced_table();
            referenced_column_id = ref_col_in->referenced_column_id();
          } else {
            referenced_table_out = in_table;
            referenced_column_id = column_id;
          }

          // automatically creates the entry if it does not exist
          std::shared_ptr<PosList> &pos_list_out = filtered_pos_lists[pos_list_in];

          if (!pos_list_out) {
            pos_list_out = std::make_shared<PosList>();
            pos_list_out->reserve(matches_in_this_chunk.size());
            std::copy(matches_in_this_chunk.begin(), matches_in_this_chunk.end(), std::back_inserter(*pos_list_out));
          }

          auto ref_col_out =
              std::make_shared<ReferenceColumn>(referenced_table_out, referenced_column_id, pos_list_out);
          chunk_out.add_column(ref_col_out);
        }

        {
          std::lock_guard<std::mutex> lock(output_mutex);
          output->add_chunk(std::move(chunk_out));
        }
      }));
      jobs.back()->schedule();
    }

    CurrentScheduler::wait_for_tasks(jobs);

    return output;
  }

 protected:
  class TableScanConstantColumnVisitable;
  class TableScanVariableColumnVisitable;
  class TableScanLikeVisitable;

  const std::shared_ptr<const AbstractOperator> _in_operator;
  std::string _filter_column_name;
  std::string _op;
  std::function<bool(T, T)> _value_comparator;
  std::function<bool(ValueID, ValueID, ValueID)> _value_id_comparator;
  const AllParameterVariant _value;
  const optional<AllTypeVariant> _value2;
  bool _is_constant_value_scan;  // indicates whether we compare two columns or a column with a (constant) value
  ScanType _type;
};

template <typename T>
class TableScan::TableScanImpl<T>::TableScanLikeVisitable : public ColumnVisitable {
 public:
  TableScanLikeVisitable(std::string like_string) {
    // convert the given SQL-like search term into a c++11 regex to use it for the actual matching
    std::string regex_string = sqllike_to_regex(like_string);
    regex = std::regex(regex_string, std::regex_constants::icase);  // case insentivity
  }

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<ScanContext>(base_context);
    const auto &column = static_cast<ValueColumn<std::string> &>(base_column);
    const auto &left = column.values();
    auto &matches_out = context->matches_out;

    if (context->chunk_offsets_in) {
      // This ValueColumn is referenced by a ReferenceColumn (i.e., is probably filtered)
      for (const ChunkOffset &offset_in_value_column : *(context->chunk_offsets_in)) {
        if (std::regex_match(left[offset_in_value_column], regex)) {
          matches_out.emplace_back(RowID{context->chunk_id, offset_in_value_column});
        }
      }
    } else {
      ChunkOffset chunk_offset = 0;
      for (const auto &value : left) {
        if (std::regex_match(value, regex)) matches_out.emplace_back(RowID{context->chunk_id, chunk_offset});
        chunk_offset++;
      }
    }
  }

  void handle_reference_column(ReferenceColumn &column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    column.visit_dereferenced<ScanContext>(*this, base_context);
  }

  void handle_dictionary_column(BaseColumn &base_column,
                                std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<ScanContext>(base_context);
    const auto &column = static_cast<DictionaryColumn<std::string> &>(base_column);
    auto &matches_out = context->matches_out;

    const BaseAttributeVector &attribute_vector = *(column.attribute_vector());

    if (context->chunk_offsets_in) {
      // first we get all attribute values
      for (const ChunkOffset &offset_in_dictionary_column : *(context->chunk_offsets_in)) {
        auto &value = column.get(offset_in_dictionary_column);
        if (std::regex_match(value, regex)) {
          matches_out.emplace_back(RowID{context->chunk_id, offset_in_dictionary_column});
        }
      }
    } else {
      // First we try to match to the dictonary so that we only have to match the regex to every unique string
      auto dictonary = column.dictionary();
      std::vector<bool> matches;
      matches.reserve(dictonary->size());
      for (auto &value : *dictonary) {
        matches.push_back(std::regex_match(value, regex));
      }
      // then we start to test the matchpattern
      for (ChunkOffset chunk_offset = 0; chunk_offset < column.size(); ++chunk_offset) {
        if (matches[attribute_vector.get(chunk_offset)]) {
          matches_out.emplace_back(RowID{context->chunk_id, chunk_offset});
        }
      }
    }
  }

 protected:
  std::regex regex;
};

// Constant TableScan (e.g. a < 1)
template <typename T>
class TableScan::TableScanImpl<T>::TableScanConstantColumnVisitable : public ColumnVisitable {
 public:
  TableScanConstantColumnVisitable(std::function<bool(T, T)> value_comparator,
                                   std::function<bool(ValueID, ValueID, ValueID)> value_id_comparator, ScanType type,
                                   T constant_value, optional<T> constant_value2)
      : _value_comparator(value_comparator),
        _value_id_comparator(value_id_comparator),
        _type(type),
        _constant_value(constant_value),
        _constant_value2(constant_value2) {}
  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<ScanContext>(base_context);
    const auto &column = static_cast<ValueColumn<T> &>(base_column);
    const auto &left = column.values();
    auto &matches_out = context->matches_out;

    T const_value = _constant_value;
    if (context->chunk_offsets_in) {
      // This ValueColumn is referenced by a ReferenceColumn (i.e., is probably filtered). We only return the matching
      // rows within the filtered column, together with their original position
      for (const ChunkOffset &offset_in_value_column : *(context->chunk_offsets_in)) {
        if (_value_comparator(left[offset_in_value_column], const_value)) {
          matches_out.emplace_back(RowID{context->chunk_id, offset_in_value_column});
        }
      }
    } else {
      ChunkOffset chunk_offset = 0;
      for (const auto &value : left) {
        if (_value_comparator(value, const_value)) matches_out.emplace_back(RowID{context->chunk_id, chunk_offset});
        chunk_offset++;
      }
    }
  }

  void handle_reference_column(ReferenceColumn &column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    column.visit_dereferenced<ScanContext>(*this, base_context);
  }

  void handle_dictionary_column(BaseColumn &base_column,
                                std::shared_ptr<ColumnVisitableContext> base_context) override {
    /*
    ValueID x;
    T A;
    optional<T> B;

    A ValueID x from the attribute vector is included in the result iff

    Operator          | Condition
    x == A            | dict.value_by_value_id(dict.lower_bound(A)) == A && x == dict.lower_bound(A)
    x != A            | dict.value_by_value_id(dict.lower_bound(A)) != A || x != dict.lower_bound(A)
    x <  A            | x < dict.lower_bound(A)
    x <= A            | x < dict.upper_bound(A)
    x >  A            | x >= dict.upper_bound(A)
    x >= A            | x >= dict.lower_bound(A)
    x between A and B | x >= dict.lower_bound(A) && x < dict.upper_bound(B)
    */

    auto context = std::static_pointer_cast<ScanContext>(base_context);
    const auto &column = static_cast<DictionaryColumn<T> &>(base_column);
    auto &matches_out = context->matches_out;

    ValueID search_vid;
    ValueID search_vid2 = INVALID_VALUE_ID;

    switch (_type) {
      case OpEquals:
      case OpNotEquals:
      case OpLessThan:
      case OpGreaterThanEquals:
        search_vid = column.lower_bound(_constant_value);
        break;

      case OpLessThanEquals:
      case OpGreaterThan:
        search_vid = column.upper_bound(_constant_value);
        break;

      case OpBetween:
        search_vid = column.lower_bound(_constant_value);
        search_vid2 = column.upper_bound(*_constant_value2);
        break;

      default:
        Fail("Unknown comparison type encountered");
    }

    if (_type == OpEquals && search_vid != INVALID_VALUE_ID &&
        column.value_by_value_id(search_vid) != _constant_value) {
      // the value is not in the dictionary and cannot be in the table
      return;
    }

    if (_type == OpNotEquals && search_vid != INVALID_VALUE_ID &&
        column.value_by_value_id(search_vid) != _constant_value) {
      // the value is not in the dictionary and cannot be in the table
      search_vid = INVALID_VALUE_ID;
    }

    const BaseAttributeVector &attribute_vector = *(column.attribute_vector());

    if (context->chunk_offsets_in) {
      for (const ChunkOffset &offset_in_dictionary_column : *(context->chunk_offsets_in)) {
        if (_value_id_comparator(attribute_vector.get(offset_in_dictionary_column), search_vid, search_vid2)) {
          matches_out.emplace_back(RowID{context->chunk_id, offset_in_dictionary_column});
        }
      }
    } else {
      // This DictionaryColumn has to be scanned in full. We directly insert the results into the list of matching
      // rows.
      for (ChunkOffset chunk_offset = 0; chunk_offset < column.size(); ++chunk_offset) {
        if (_value_id_comparator(attribute_vector.get(chunk_offset), search_vid, search_vid2)) {
          matches_out.emplace_back(RowID{context->chunk_id, chunk_offset});
        }
      }
    }
  }

 protected:
  std::function<bool(T, T)> _value_comparator;
  std::function<bool(ValueID, ValueID, ValueID)> _value_id_comparator;
  ScanType _type;
  T _constant_value;
  optional<T> _constant_value2;
};

// Variable TableScan (e.g. a < b)
template <typename T>
class TableScan::TableScanImpl<T>::TableScanVariableColumnVisitable : public ColumnVisitable {
 public:
  TableScanVariableColumnVisitable(std::function<bool(T, T)> value_comparator,
                                   std::function<bool(ValueID, ValueID, ValueID)> value_id_comparator)
      : _value_comparator(value_comparator), _value_id_comparator(value_id_comparator) {}
  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<ScanContext>(base_context);
    const auto &column = static_cast<ValueColumn<T> &>(base_column);
    const auto &left = column.values();
    auto &matches_out = context->matches_out;

    if (context->chunk_offsets_in) {
      // This ValueColumn is referenced by a ReferenceColumn (i.e., is probably filtered). We only return the matching
      // rows within the filtered column, together with their original position
      const auto &right = context->values;
      for (const ChunkOffset &offset_in_value_column : *(context->chunk_offsets_in)) {
        if (_value_comparator(left[offset_in_value_column], right[offset_in_value_column])) {
          matches_out.emplace_back(RowID{context->chunk_id, offset_in_value_column});
        }
      }

    } else {
      // This ValueColumn has to be scanned in full. We directly insert the results into the list of matching rows.
      ChunkOffset chunk_offset = 0;
      const auto &right = context->values;
      for (const auto &value : left) {
        if (_value_comparator(value, right[chunk_offset]))
          matches_out.emplace_back(RowID{context->chunk_id, chunk_offset});
        chunk_offset++;
      }
    }
  }

  void handle_reference_column(ReferenceColumn &column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    column.visit_dereferenced<ScanContext>(*this, base_context);
  }

  void handle_dictionary_column(BaseColumn &base_column,
                                std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<ScanContext>(base_context);
    const auto &column = static_cast<DictionaryColumn<T> &>(base_column);
    auto &matches_out = context->matches_out;

    const auto &left = column.materialize_values();  // also materializing the values on the left side
    const auto &right = context->values;
    if (context->chunk_offsets_in) {
      for (const ChunkOffset &offset_in_value_column : *(context->chunk_offsets_in)) {
        if (_value_comparator(left[offset_in_value_column], right[offset_in_value_column])) {
          matches_out.emplace_back(RowID{context->chunk_id, offset_in_value_column});
        }
      }
    } else {
      ChunkOffset chunk_offset = 0;
      for (const auto &value : left) {
        if (_value_comparator(value, right[chunk_offset]))
          matches_out.emplace_back(RowID{context->chunk_id, chunk_offset});
        chunk_offset++;
      }
    }
  }

 protected:
  std::function<bool(T, T)> _value_comparator;
  std::function<bool(ValueID, ValueID, ValueID)> _value_id_comparator;
};

}  // namespace opossum
