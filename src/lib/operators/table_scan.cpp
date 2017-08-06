#include "table_scan.hpp"

#include <unordered_map>

#include <boost/hana/type.hpp>
#include <boost/hana/or.hpp>

#include "storage/iterables/attribute_vector_iterable.hpp"
#include "storage/iterables/value_column_iterable.hpp"
#include "storage/iterables/dictionary_column_iterable.hpp"
#include "storage/iterables/reference_column_iterable.hpp"
#include "storage/iterables/constant_value_iterable.hpp"
#include "utils/binary_operators.hpp"
#include "resolve_column_type.hpp"


namespace opossum {

namespace hana = boost::hana;

/**
 * @brief The actual scan
 */
struct Scan {
  Scan(const ChunkID chunk_id, PosList & matches_out)
      : _chunk_id{chunk_id}, _matches_out{matches_out} {}

  template <typename LeftIterator, typename RightIterator, typename Comparator>
  void operator()(const Comparator & comparator, LeftIterator left_it, LeftIterator left_end, RightIterator right_it) {
    for (; left_it != left_end; ++left_it, ++right_it) {
      const auto left = *left_it;
      const auto right = *right_it;
      
      if (left.is_null() || right.is_null()) continue;

      if (comparator(left.value(), right.value())) {
        _matches_out.push_back(RowID{_chunk_id, left.chunk_offset()});
      }
    }
  }

  const ChunkID _chunk_id;
  PosList & _matches_out;
};

class AbstractScan {
 public:
  AbstractScan(std::shared_ptr<const Table> in_table, const ScanType scan_type) : _in_table{in_table}, _scan_type{scan_type} {}
  virtual ~AbstractScan() = default;

  virtual PosList scan_chunk(const ChunkID & chunk_id) = 0;

 protected:
  template <typename Type>
  auto _create_iterable_from_column(ValueColumn<Type> & column) { return ValueColumnIterable<Type>{column}; }

  template <typename Type>
  auto _create_iterable_from_column(DictionaryColumn<Type> & column) { return DictionaryColumnIterable<Type>{column}; }

  template <typename Type>
  auto _create_iterable_from_column(ReferenceColumn & column) { return ReferenceColumnIterable<Type>{column}; }
  
  template <typename LeftIterable, typename RightIterable>
  void _scan(LeftIterable & left_iterable, RightIterable & right_iterable, const ChunkID chunk_id, PosList & matches_out) {
    left_iterable.execute_for_all([&] (auto left_it, auto left_end) {
      right_iterable.execute_for_all([&] (auto right_it, auto right_end) {
        resolve_operator_type(_scan_type, [&] (auto comparator) {
          Scan{chunk_id, matches_out}(comparator, left_it, left_end, right_it); 
        });
      });
    });
  }
  
  template <typename LeftIterable, typename RightIterable, typename Comparator>
  void _scan(const Comparator & comparator, LeftIterable & left_iterable, RightIterable & right_iterable, const ChunkID chunk_id, PosList & matches_out) {
    left_iterable.execute_for_all([&] (auto left_it, auto left_end) {
      right_iterable.execute_for_all([&] (auto right_it, auto right_end) {
        Scan{chunk_id, matches_out}(comparator, left_it, left_end, right_it);
      });
    });
  }

 protected:
  const std::shared_ptr<const Table> _in_table;
  const ScanType _scan_type;
};


// 1 data column (dictionary, value)
class DataColumnScan : public AbstractScan {
 public:
  DataColumnScan(std::shared_ptr<const Table> in_table, const ScanType & scan_type,
                 const ColumnID left_column_id, const AllTypeVariant & right_value)
      : AbstractScan{in_table, scan_type}, _left_column_id{left_column_id}, _right_value{right_value} {}

  PosList scan_chunk(const ChunkID & chunk_id) override {
    const auto & chunk = _in_table->get_chunk(chunk_id);

    const auto left_column_type = _in_table->column_type(_left_column_id);
    const auto left_column = chunk.get_column(_left_column_id);

    auto matches_out = PosList{};

    resolve_column_type(left_column_type, *left_column, [&] (auto type, auto &typed_left_column) {
      using Type = typename decltype(type)::type;

      auto left_column_iterable = _create_iterable_from_column(typed_left_column);
      auto right_value_iterable = ConstantValueIterable<Type>{_right_value};

      _scan(left_column_iterable, right_value_iterable, chunk_id, matches_out);
    });

    return matches_out;
  }

 private:
  const ColumnID _left_column_id;
  const AllTypeVariant _right_value;
};

// 1 data column
class SingleColumnScan : public AbstractScan, public ColumnVisitable {
 public:
  struct Context : public ColumnVisitableContext {
    Context(const ChunkID chunk_id, PosList & matches_out) : _chunk_id{chunk_id}, _matches_out{matches_out}{}

    Context(const ChunkID chunk_id, PosList & matches_out, std::unique_ptr<std::vector<std::pair<ChunkOffset, ChunkOffset>>> mapped_chunk_offsets)
        : _chunk_id{chunk_id}, _matches_out{matches_out}, _mapped_chunk_offsets{std::move(mapped_chunk_offsets)} {}

    const ChunkID _chunk_id;
    PosList & _matches_out;

    std::unique_ptr<std::vector<std::pair<ChunkOffset, ChunkOffset>>> _mapped_chunk_offsets;
  };

 public:
  SingleColumnScan(std::shared_ptr<const Table> in_table, const ScanType & scan_type,
             const ColumnID left_column_id, const AllTypeVariant & right_value)
      : AbstractScan{in_table, scan_type}, _left_column_id{left_column_id}, _right_value{right_value} {}

  PosList scan_chunk(const ChunkID & chunk_id) override {
    const auto & chunk = _in_table->get_chunk(chunk_id);

    const auto left_column_type = _in_table->column_type(_left_column_id);
    const auto left_column = chunk.get_column(_left_column_id);

    auto matches_out = PosList{};

    auto context = std::make_shared<Context>(chunk_id, matches_out);

    left_column->visit(*this, context);

    return std::move(context->_matches_out);
  }

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<Context>(base_context);

    const auto left_column_type = _in_table->column_type(_left_column_id);

    resolve_type(left_column_type, [&] (auto type) {
      using Type = typename decltype(type)::type;

      auto & left_column = static_cast<ValueColumn<Type> &>(base_column);

      auto left_column_iterable = _create_iterable_from_column(left_column, context->_mapped_chunk_offsets.get());
      auto right_value_iterable = ConstantValueIterable<Type>{_right_value};

      _scan(left_column_iterable, right_value_iterable, context->_chunk_id, context->_matches_out);
    });
  }

  void handle_dictionary_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<Context>(base_context);
    auto & matches_out = context->_matches_out;
    auto & left_column = static_cast<const UntypedDictionaryColumn &>(base_column);

    // TODO(mjendruk): Find a good heuristic for when simply scanning column is faster (dictionary size -> attribute vector size)

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

    if (_right_value_matches_all(left_column, search_value_id)) {
      const auto & attribute_vector = *left_column.attribute_vector();

      auto attribute_vector_iterable = AttributeVectorIterable{attribute_vector, context->_mapped_chunk_offsets.get()};

      attribute_vector_iterable.execute_for_all([&] (auto left_it, auto left_end) {
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

    const auto & attribute_vector = *left_column.attribute_vector();

    auto attribute_vector_iterable = AttributeVectorIterable{attribute_vector, context->_mapped_chunk_offsets.get()};
    auto constant_value_iterable = ConstantValueIterable<ValueID>{search_value_id};

    _resolve_scan_type([&] (auto comparator) {
      _scan(comparator, attribute_vector_iterable, constant_value_iterable, context->_chunk_id, matches_out);
    });
  }

  void handle_reference_column(ReferenceColumn &left_column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<Context>(base_context);
    const ChunkID chunk_id = context->_chunk_id;
    auto & matches_out = context->_matches_out;

    // TODO(mjendruk): Find a good estimates for when it’s better simply iterate over the column

    auto chunk_offsets_by_chunk_id = _split_position_list_by_chunk_id(*left_column.pos_list());

    for (auto & pair : chunk_offsets_by_chunk_id) {
      const auto &chunk_id = pair.first;
      auto &mapped_chunk_offsets = pair.second;

      const auto &chunk = left_column.referenced_table()->get_chunk(chunk_id);
      auto referenced_column = chunk.get_column(left_column.referenced_column_id());

      auto mapped_chunk_offsets_ptr = std::make_unique<std::vector<std::pair<ChunkOffset, ChunkOffset>>>(std::move(mapped_chunk_offsets));

      auto new_context = std::make_shared<Context>(chunk_id, matches_out, std::move(mapped_chunk_offsets_ptr));
      referenced_column->visit(*this, new_context);
    }
  }

 private:
  /**
   * @defgroup Methods used for handling value columns
   * @{
   */

  template <typename Type>
  auto _create_iterable_from_column(ValueColumn<Type> & column, const std::vector<std::pair<ChunkOffset, ChunkOffset>> * mapped_chunk_offsets) { 
    return ValueColumnIterable<Type>{column, mapped_chunk_offsets}; 
  }

  /**@}*/

 private:
  /**
   * @defgroup Methods used for handling dictionary columns
   * @{
   */

  ValueID _get_search_value_id(const UntypedDictionaryColumn & column) {
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

  bool _right_value_matches_all(const UntypedDictionaryColumn & column, const ValueID search_value_id) {
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

  bool _right_value_matches_none(const UntypedDictionaryColumn & column, const ValueID search_value_id) {
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
  void _resolve_scan_type(const Functor & func) {
    switch (_scan_type) {
      case ScanType::OpEquals:
        return func(Equal{});

      case ScanType::OpNotEquals:
        return func(NotEqual{});

      case ScanType::OpLessThan:
      case ScanType::OpLessThanEquals:
        return func(Less{});

      case ScanType::OpGreaterThan:
      case ScanType::OpGreaterThanEquals:
        return func(GreaterEqual{});

      default:
        Fail("Unsupported comparison type encountered");
    }
  }

  /**@}*/

 private:
  /**
   * @defgroup Methods used for handling reference columns
   * @{
   */

  using ChunkOffsetsByChunkID = std::unordered_map<ChunkID, std::vector<std::pair<ChunkOffset, ChunkOffset>>, std::hash<decltype(ChunkID().t)>>;
  
  ChunkOffsetsByChunkID _split_position_list_by_chunk_id(const PosList & pos_list) {
    auto chunk_offsets_by_chunk_id = ChunkOffsetsByChunkID{};

    for (auto chunk_offset = 0u; chunk_offset < pos_list.size(); ++chunk_offset) {
      const auto row_id = pos_list[chunk_offset];

      auto & mapped_chunk_offsets = chunk_offsets_by_chunk_id[row_id.chunk_id];
      mapped_chunk_offsets.emplace_back(chunk_offset, row_id.chunk_offset);
    }

    return chunk_offsets_by_chunk_id;
  }

  /**@}*/

 private:
  const ColumnID _left_column_id;
  const AllTypeVariant _right_value;
};


// 1 reference column
class ReferenceColumnScan : public AbstractScan {
 public:
  ReferenceColumnScan(std::shared_ptr<const Table> in_table, const ScanType & scan_type,
                      const ColumnID left_column_id, const AllTypeVariant & right_value)
      : AbstractScan{in_table, scan_type}, _left_column_id{left_column_id}, _right_value{right_value} {}

  PosList scan_chunk(const ChunkID & chunk_id) override {
    const auto & chunk = _in_table->get_chunk(chunk_id);

    const auto left_column_type = _in_table->column_type(_left_column_id);
    const auto left_column = std::dynamic_pointer_cast<ReferenceColumn>(chunk.get_column(_left_column_id));

    DebugAssert(left_column != nullptr, "Left column must be of type ReferenceColumn");

    auto matches_out = PosList{};

    resolve_type(left_column_type, [&] (auto type) {
      using Type = typename decltype(type)::type;

      auto left_column_iterable = ReferenceColumnIterable<Type>(*left_column);
      auto right_value_iterable = ConstantValueIterable<Type>(_right_value);

      _scan(left_column_iterable, right_value_iterable, chunk_id, matches_out);
    });

    return matches_out;
  }

 private:
  const ColumnID _left_column_id;
  const AllTypeVariant _right_value;
};


// 2 data columns (dictionary, value)
class DataColumnComparisonScan : public AbstractScan {
 public:
  DataColumnComparisonScan(std::shared_ptr<const Table> in_table, const ScanType & scan_type,
                           const ColumnID left_column_id, const ColumnID right_column_id)
      : AbstractScan{in_table, scan_type}, _left_column_id{left_column_id}, _right_column_id{right_column_id} {}

  PosList scan_chunk(const ChunkID & chunk_id) override {
    const auto & chunk = _in_table->get_chunk(chunk_id);
    const auto left_column_type = _in_table->column_type(_left_column_id);
    const auto right_column_type = _in_table->column_type(_right_column_id);

    const auto left_column = chunk.get_column(_left_column_id);
    const auto right_column = chunk.get_column(_right_column_id);

    auto matches_out = PosList{};

    resolve_column_type(left_column_type, *left_column, [&] (auto left_type, auto &typed_left_column) {
      resolve_column_type(right_column_type, *right_column, [&] (auto right_type, auto &typed_right_column) {
        constexpr auto left_is_string_column = (left_type == hana::type_c<std::string>);
        constexpr auto right_is_string_column = (right_type == hana::type_c<std::string>);
        constexpr auto neither_is_string_column = !left_is_string_column && !right_is_string_column;
        constexpr auto both_are_string_columns = left_is_string_column && right_is_string_column;

        if constexpr (neither_is_string_column || both_are_string_columns) {
          auto left_column_iterable = _create_iterable_from_column(typed_left_column);
          auto right_column_iterable = _create_iterable_from_column(typed_right_column);

          _scan(left_column_iterable, right_column_iterable, chunk_id, matches_out);
        } else {
          Fail("std::string cannot be compared to numerical type!");
        }
      });
    });

    return matches_out;
  }

 private:
  const ColumnID _left_column_id;
  const ColumnID _right_column_id;
};


// 2 data columns (dictionary, value)
class ReferenceColumnComparisonScan : public AbstractScan {
 public:
  ReferenceColumnComparisonScan(std::shared_ptr<const Table> in_table, const ScanType & scan_type,
                                const ColumnID left_column_id, const ColumnID right_column_id)
      : AbstractScan{in_table, scan_type}, _left_column_id{left_column_id}, _right_column_id{right_column_id} {}

  PosList scan_chunk(const ChunkID & chunk_id) override {
    const auto & chunk = _in_table->get_chunk(chunk_id);
    const auto left_column_type = _in_table->column_type(_left_column_id);
    const auto right_column_type = _in_table->column_type(_right_column_id);

    const auto left_column = std::dynamic_pointer_cast<ReferenceColumn>(chunk.get_column(_left_column_id));
    const auto right_column = std::dynamic_pointer_cast<ReferenceColumn>(chunk.get_column(_right_column_id));

    DebugAssert(left_column != nullptr, "Left column must be of type ReferenceColumn");
    DebugAssert(right_column != nullptr, "Right column must be of type ReferenceColumn");

    auto matches_out = PosList{};

    resolve_type(left_column_type, [&] (auto left_type) {
      resolve_type(right_column_type, [&] (auto right_type) {
        constexpr auto left_is_string_column = (left_type == hana::type_c<std::string>);
        constexpr auto right_is_string_column = (right_type == hana::type_c<std::string>);
        constexpr auto neither_is_string_column = !left_is_string_column && !right_is_string_column;
        constexpr auto both_are_string_columns = left_is_string_column && right_is_string_column;

        if constexpr (neither_is_string_column || both_are_string_columns) {
          using LeftType = typename decltype(left_type)::type;
          using RightType = typename decltype(right_type)::type;

          auto left_column_iterable = _create_iterable_from_column<LeftType>(*left_column);
          auto right_column_iterable = _create_iterable_from_column<RightType>(*right_column);

          _scan(left_column_iterable, right_column_iterable, chunk_id, matches_out);
        } else {
          Fail("std::string cannot be compared to numerical type!");
        }
      });
    });

    return matches_out;
  }

 private:
  const ColumnID _left_column_id;
  const ColumnID _right_column_id;
};


TableScan::TableScan(const std::shared_ptr<AbstractOperator> in, const std::string &left_column_name,
                           const ScanType scan_type, const AllParameterVariant right_parameter,
                           const optional<AllTypeVariant> right_value2)
    : AbstractReadOnlyOperator{in}, _left_column_name{left_column_name}, _scan_type{scan_type},
      _right_parameter{right_parameter}, _right_value2{right_value2} {}

TableScan::~TableScan() = default;

const std::string TableScan::name() const { return "TableScan"; }

uint8_t TableScan::num_in_tables() const { return 1; }

uint8_t TableScan::num_out_tables() const { return 1; }

std::shared_ptr<AbstractOperator> TableScan::recreate(const std::vector<AllParameterVariant> &args) const {
  // Replace value in the new operator, if it’s a parameter and an argument is available.
  if (is_placeholder(_right_parameter)) {
    const auto index = boost::get<ValuePlaceholder>(_right_parameter).index();
    if (index < args.size()) {
      return std::make_shared<TableScan>(_input_left->recreate(args), _left_column_name, _scan_type, args[index], _right_value2);
    }
  }
  return std::make_shared<TableScan>(_input_left->recreate(args), _left_column_name, _scan_type, _right_parameter, _right_value2);
}

std::shared_ptr<const Table> TableScan::on_execute()
{
  _in_table = input_table_left();

  init_scan();
  init_output_table();

  std::mutex output_mutex;

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(_in_table->chunk_count());

  for (ChunkID chunk_id{0u}; chunk_id < _in_table->chunk_count(); ++chunk_id) {
    auto job_task = std::make_shared<JobTask>([=, &output_mutex]() {

      const auto matches_out = std::make_shared<PosList>(_scan->scan_chunk(chunk_id));

      Chunk chunk_out;

      if (_is_reference_table) {
        const auto & chunk_in = _in_table->get_chunk(chunk_id);

        auto filtered_pos_lists = std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>>{};

        // TODO(mjendruk): implement short cut for pos_list is the same as the scanned column’s

        for (ColumnID column_id{0u}; column_id < _in_table->col_count(); ++column_id) {
          auto column_in = chunk_in.get_column(column_id);

          auto ref_column_in = std::dynamic_pointer_cast<const ReferenceColumn>(column_in);
          DebugAssert(ref_column_in != nullptr, "All columns should be of type ReferenceColumn.");

          const auto pos_list_in = ref_column_in->pos_list();

          const auto table_out = ref_column_in->referenced_table();
          const auto column_id_out = ref_column_in->referenced_column_id();

          auto & filtered_pos_list = filtered_pos_lists[pos_list_in];

          if (!filtered_pos_list) {  
            filtered_pos_list = std::make_shared<PosList>();
            filtered_pos_list->reserve(matches_out->size());
            
            for (const auto & match : *matches_out) {
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

void TableScan::init_scan()
{
  const auto left_column_id = _in_table->column_id_by_name(_left_column_name);

  DebugAssert(_in_table->chunk_count() > 0u, "Input table must contain at least 1 chunk.");
  const auto & first_chunk = _in_table->get_chunk(ChunkID{0u});

  _is_reference_table = first_chunk.get_column(left_column_id)->is_reference_column();

  if (is_variant(_right_parameter)) {
    const auto right_value = boost::get<AllTypeVariant>(_right_parameter);

    DebugAssert(!is_null(right_value), "Right value must not be NULL.");

    _scan = std::make_unique<SingleColumnScan>(_in_table, _scan_type, left_column_id, right_value);

    // if (_is_reference_table) {
    //   _scan = std::make_unique<ReferenceColumnScan>(_in_table, _scan_type, left_column_id, right_value);
    // } else {
    //   _scan = std::make_unique<DataColumnScan>(_in_table, _scan_type, left_column_id, right_value);
    // }
  } else /* is_column_name(_right_parameter) */ {
    const auto right_column_name = boost::get<ColumnName>(_right_parameter);
    const auto right_column_id = _in_table->column_id_by_name(right_column_name);

    if (_is_reference_table) {
      _scan = std::make_unique<ReferenceColumnComparisonScan>(_in_table, _scan_type, left_column_id, right_column_id);
    } else {
      _scan = std::make_unique<DataColumnComparisonScan>(_in_table, _scan_type, left_column_id, right_column_id);
    }
  }
}

void TableScan::init_output_table()
{
  _output_table = std::make_shared<Table>();

  for (ColumnID column_id{0}; column_id < _in_table->col_count(); ++column_id) {
    _output_table->add_column_definition(_in_table->column_name(column_id), _in_table->column_type(column_id));
  }
}

}  // namespace opossum
