#include "new_table_scan.hpp"

#include <type_traits>

#include <boost/hana/type.hpp>

#include "utils/binary_operators.hpp"
#include "utils/type_traits.hpp"

namespace opossum {

/**
 * @brief The actual scan
 */
struct Scan {
  Scan(const ChunkID chunk_id, std::vector<RowID> & matches_out)
      : _chunk_id{chunk_id}, _matches_out{matches_out} {}

  template <typename LeftIterator, typename RightIterator, typename Comparator>
  void operator()(const Comparator & comparator, LeftIterator left_it, LeftIterator left_end, RightIterator right_it) {
    auto chunk_offset = ChunkOffset{0u};
    for (; left_it != left_end; ++left_it, ++right_it, ++chunk_offset) {
      if ((*left_it).is_null() || (*right_it).is_null()) continue;

      if (comparator((*left_it).value(), (*right_it).value())) {
        _matches_out.push_back(RowID{_chunk_id, chunk_offset});
      }
    }
  }

  const ChunkID _chunk_id;
  std::vector<RowID> & _matches_out;
};

class NewTableScan::AbstractScan {
 public:
  AbstractScan(std::shared_ptr<const Table> in_table);

  virtual std::vector<ChunkOffset> scan_chunk(const ChunkID & chunk_id) = 0;

 protected:
  const std::shared_ptr<const Table> _in_table;
};

// 2 data columns (dictionary, value)
class DataColumnComparisonScan : public NewTableScan::AbstractScan {
 public:
  ConstantValueScan(std::shared_ptr<const Table> in_table, const std::string & left_column_id,
                    const std::string & right_column_id, const ScanType & scan_type)
      : AbstractScan{in_table}, _left_column_id{left_column_id}, _right_column_id{right_column_id},
        _scan_type{scan_type} {}

  std::vector<ChunkOffset> scan_chunk(const ChunkID & chunk_id) override {
    const auto & chunk = _in_table->get_chunk(chunk_id);
    const auto left_column_type = _in_table->column_type(_left_column_id);
    const auto right_column_type = _in_table->column_type(_right_column_id);

    const auto left_column = chunk.get_column(_left_column_id);
    const auto right_column = chunk.get_column(_right_column_id);

    auto matches_out = std::vector<RowID>{};
    auto scan = Scan{chunk_id, matches_out};

    // TODO(mjendruk): Exclude comparing strings to numerical types ...
    resolve_column_type(left_column_type, left_column, [&] (auto typed_left_column) {
      resolve_column_type(right_column_type, right_column, [&] (auto typed_right_column) {
        resolve_operator_type(_scan_type, [&] (auto comparator) {
          using LeftColumn = std::decay_t<decltype(typed_left_column)>;
          using RightColumn = std::decay_t<decltype(typed_right_column)>;

          // Don’t compile the code for reference columns
          constexpr auto not_reference_columns = !LeftColumn::is_reference_column() && !RightColumn::is_reference_column();
          if constexpr (not_reference_columns) {

            auto left_column_iterable = create_iterable_from_column(typed_left_column);
            auto right_column_iterable = create_iterable_from_column(typed_right_column);

            _left_column_iterable.execute_for_all([&] (auto left_it, auto left_end) {
              right_column_iterable.execute_for_all([&] (auto right_it, auto right_end) {
                scan(comparator, left_it, left_end, right_it);
              });
            });

          }
        });
      });
    });

    return matches_out;
  }

  template <typename Functor>
  void resolve_operator_type(const ScanType scan_type, const Functor & func) {
    switch (_scan_type) {
      case ScanType::OpEquals:
        func(Equal{});
        break;

      case ScanType::OpNotEquals:
        func(NotEqual{});
        break;

      case ScanType::OpLessThan:
        func(Less{});
        break;

      case ScanType::OpLessThanEquals:
        func(LessEqual{});
        break;

      case ScanType::OpGreaterThan:
        func(Greater{});
        break;

      case ScanType::OpGreaterThanEquals:
        func(GreaterEqual{});
        break;

      case ScanType::OpBetween:
        Fail("This method should only be called when ScanType::OpBetween has been ruled out.");

      case ScanType::OpLike:
        Fail("This method should only be called when ScanType::OpLike has been ruled out.");

      default:
        Fail("Unsupported operator.");
    }
  }

  template <typename Type>
  auto create_iterable_from_column(ValueColumn<Type> & column) { return ValueColumnIterable<Type>(column); }

  template <typename Type>
  auto create_iterable_from_column(DictionaryColumn<Type> & column) { return DictionaryColumnIterable<Type>(column); }

 private:
  const ColumnID _left_column_id;
  const ColumnID _right_column_id;
  const ScanType _scan_type;
};

NewTableScan::NewTableScan(const std::shared_ptr<AbstractOperator> in, const std::string &left_column_name,
                           const ScanType scan_type, const AllParameterVariant right_parameter,
                           const optional<AllTypeVariant> right_value2)
    : AbstractReadOnlyOperator{in}, _left_column_name{left_column_name}, _scan_type{scan_type},
      _right_parameter{right_parameter}, _right_value2{right_value2} {}

const std::string NewTableScan::name() const { return "NewTableScan"; }

uint8_t NewTableScan::num_in_tables() const { return 1; }

uint8_t NewTableScan::num_out_tables() const { return 1; }

std::shared_ptr<AbstractOperator> NewTableScan::recreate(const std::vector<AllParameterVariant> &args) const {
  return nullptr;
}

std::shared_ptr<const Table> NewTableScan::on_execute()
{
  _in_table = input_table_left();

  init_scan();

  // TODO(mjendruk): Implement stuff here ...

  return nullptr;
}

void NewTableScan::init_scan()
{
  const auto left_column_id = _in_table->column_id_by_name(_left_column_name);

  DebugAssert(_in_table->chunk_count() > 0u, "Input table must contain at least 1 chunk.");
  const auto & first_chunk = _in_table->get_chunk(0u);

  const auto is_reference_table = first_chunk.get_column(left_column_id)->is_reference_column();

  if (is_variant(_right_parameter)) {
    // TODO(mjendruk): Implement scan for constant value
  } else /* is_column_name(_right_parameter) */ {
    const auto right_column_name = boost::get<ColumnName>(_right_parameter);
    const auto right_column_id = _in_table->column_id_by_name(right_column_name);

    if (is_reference_table) {
      // TODO(mjendruk): Implement scan for two reference columns
    } else {
      _scan = std::make_unique<DataColumnComparisonScan>(_in_table, left_column_id, right_column_id, _scan_type);
    }
  }
}

}  // namespace opossum
