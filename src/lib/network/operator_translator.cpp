#include "operator_translator.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "operators/abstract_operator.hpp"
#include "operators/difference.hpp"
#include "operators/export_binary.hpp"
#include "operators/export_csv.hpp"
#include "operators/get_table.hpp"
#include "operators/import_csv.hpp"
#include "operators/index_column_scan.hpp"
#include "operators/join_nested_loop_a.hpp"
#include "operators/print.hpp"
#include "operators/product.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_all.hpp"
#include "scheduler/operator_task.hpp"

#include "common.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

inline AllTypeVariant translate_variant(const proto::Variant& variant) {
  switch (variant.variant_case()) {
    case proto::Variant::kValueInt:
      return variant.value_int();
    case proto::Variant::kValueFloat:
      return variant.value_float();
    case proto::Variant::kValueString:
      return variant.value_string();
    case proto::Variant::kValueDouble:
      return variant.value_double();
    case proto::Variant::kValueLong:
      return variant.value_long();
    default:
      Fail("Unknown AllTypeVariant in operator_translator");
      return {};
  }
}

inline optional<AllTypeVariant> translate_optional_variant(const proto::Variant& variant) {
  if (variant.variant_case() == proto::Variant::VARIANT_NOT_SET)
    return nullopt;
  else
    return translate_variant(variant);
}

inline ScanType translate_scan_type(const proto::ScanType& scan_type) {
  switch (scan_type) {
    case proto::ScanType::OpEquals:
      return ScanType::OpEquals;
    case proto::ScanType::OpNotEquals:
      return ScanType::OpNotEquals;
    case proto::ScanType::OpLessThan:
      return ScanType::OpLessThan;
    case proto::ScanType::OpLessThanEquals:
      return ScanType::OpLessThanEquals;
    case proto::ScanType::OpGreaterThan:
      return ScanType::OpGreaterThan;
    case proto::ScanType::OpGreaterThanEquals:
      return ScanType::OpGreaterThanEquals;
    case proto::ScanType::OpBetween:
      return ScanType::OpBetween;
    case proto::ScanType::OpLike:
      return ScanType::OpLike;
    default:
      Fail("Unsupported ScanType.");
      return {};
  }
}

inline std::shared_ptr<OperatorTask> OperatorTranslator::translate(
    const proto::ProjectionOperator& projection_operator) {
  const auto column_ids_field = projection_operator.column_id();
  auto column_ids = std::vector<ColumnID>(std::begin(column_ids_field), std::end(column_ids_field));
  Assert((projection_operator.has_input_operator()), "Missing Input Operator in Projection.");

  auto input_task = translate_proto(projection_operator.input_operator());

  Projection::ColumnExpressions column_expressions;
  column_expressions.reserve(column_ids.size());
  for (const auto column_id : column_ids) {
    column_expressions.emplace_back(Expression::create_column(ColumnID{column_id}));
  }

  auto projection = std::make_shared<Projection>(input_task->get_operator(), column_expressions);
  auto projection_task = std::make_shared<OperatorTask>(projection);
  input_task->set_as_predecessor_of(projection_task);
  _tasks.push_back(projection_task);

  return projection_task;
}

inline std::shared_ptr<OperatorTask> OperatorTranslator::translate(const proto::ProductOperator& product_operator) {
  Assert((product_operator.has_left_operator()), "Missing left Operator in Product.");
  Assert((product_operator.has_right_operator()), "Missing right Operator in Product.");

  auto input_left_task = translate_proto(product_operator.left_operator());
  auto input_right_task = translate_proto(product_operator.right_operator());

  auto product = std::make_shared<Product>(input_left_task->get_operator(), input_right_task->get_operator());
  auto product_task = std::make_shared<OperatorTask>(product);
  input_left_task->set_as_predecessor_of(product_task);
  input_right_task->set_as_predecessor_of(product_task);
  _tasks.push_back(product_task);

  return product_task;
}

inline std::shared_ptr<OperatorTask> OperatorTranslator::translate(
    const proto::NestedLoopJoinOperator& nested_loop_join_operator) {
  Assert((nested_loop_join_operator.has_left_operator()), "Missing left Operator in Nested Loop Join.");
  Assert((nested_loop_join_operator.has_right_operator()), "Missing right Operator in Nested Loop Join.");

  auto input_left_task = translate_proto(nested_loop_join_operator.left_operator());
  auto input_right_task = translate_proto(nested_loop_join_operator.right_operator());
  const auto& scan_type = translate_scan_type(nested_loop_join_operator.op());

  JoinMode join_mode;
  switch (nested_loop_join_operator.mode()) {
    case proto::NestedLoopJoinOperator::Inner:
      join_mode = JoinMode::Inner;
      break;
    case proto::NestedLoopJoinOperator::Left:
      join_mode = JoinMode::Left;
      break;
    case proto::NestedLoopJoinOperator::Right:
      join_mode = JoinMode::Right;
      break;
    case proto::NestedLoopJoinOperator::Outer:
      join_mode = JoinMode::Outer;
      break;
    case proto::NestedLoopJoinOperator::Cross:
      join_mode = JoinMode::Cross;
      break;
    case proto::NestedLoopJoinOperator::Natural:
      join_mode = JoinMode::Natural;
      break;
    case proto::NestedLoopJoinOperator::Self:
      join_mode = JoinMode::Self;
      break;
    default:
      Fail("Unknown join mode for nested loop join operator in operator_translator");
  }

  optional<std::pair<ColumnID, ColumnID>> join_columns;
  if (nested_loop_join_operator.has_left_column_id() && nested_loop_join_operator.has_right_column_id()) {
    join_columns =
        std::make_pair(ColumnID{static_cast<ColumnID::base_type>(nested_loop_join_operator.left_column_id().value())},
                       ColumnID{static_cast<ColumnID::base_type>(nested_loop_join_operator.right_column_id().value())});
  } else {
    DebugAssert(!nested_loop_join_operator.has_left_column_id() && !nested_loop_join_operator.has_right_column_id(),
                "Neither or both columns of the join condition have to be specified.");
  }

  Assert(join_columns, "Can't create join without join columns");
  auto nested_loop_join = std::make_shared<JoinNestedLoopA>(
      input_left_task->get_operator(), input_right_task->get_operator(), join_mode, *join_columns, scan_type);

  auto nested_loop_join_task = std::make_shared<OperatorTask>(nested_loop_join);
  input_left_task->set_as_predecessor_of(nested_loop_join_task);
  input_right_task->set_as_predecessor_of(nested_loop_join_task);
  _tasks.push_back(nested_loop_join_task);

  return nested_loop_join_task;
}

inline std::shared_ptr<OperatorTask> OperatorTranslator::translate(
    const proto::TableScanOperator& table_scan_operator) {
  const auto column_id = ColumnID{static_cast<ColumnID::base_type>(table_scan_operator.column_id())};
  const auto& scan_type = translate_scan_type(table_scan_operator.filter_operator());
  Assert((table_scan_operator.has_input_operator()), "Missing Input Operator in Table Scan.");

  auto input_task = translate_proto(table_scan_operator.input_operator());

  const auto value = translate_variant(table_scan_operator.value());
  const auto value2 = translate_optional_variant(table_scan_operator.value2());

  auto table_scan = std::make_shared<TableScan>(input_task->get_operator(), column_id, scan_type, value, value2);
  auto scan_task = std::make_shared<OperatorTask>(table_scan);
  input_task->set_as_predecessor_of(scan_task);
  _tasks.push_back(scan_task);

  return scan_task;
}

inline std::shared_ptr<OperatorTask> OperatorTranslator::translate(
    const proto::IndexColumnScanOperator& index_column_scan_operator) {
  const auto column_id = ColumnID{static_cast<ColumnID::base_type>(index_column_scan_operator.column_id())};
  const auto& scan_type = translate_scan_type(index_column_scan_operator.filter_operator());
  Assert((index_column_scan_operator.has_input_operator()), "Missing Input Operator in Index Column Scan.");

  auto input_task = translate_proto(index_column_scan_operator.input_operator());

  const auto value = translate_variant(index_column_scan_operator.value());
  const auto value2 = translate_optional_variant(index_column_scan_operator.value2());

  auto index_column_scan =
      std::make_shared<IndexColumnScan>(input_task->get_operator(), column_id, scan_type, value, value2);
  auto index_column_scan_task = std::make_shared<OperatorTask>(index_column_scan);
  input_task->set_as_predecessor_of(index_column_scan_task);
  _tasks.push_back(index_column_scan_task);

  return index_column_scan_task;
}

inline std::shared_ptr<OperatorTask> OperatorTranslator::translate(const proto::GetTableOperator& get_table_operator) {
  auto get_table = std::make_shared<GetTable>(get_table_operator.table_name());
  auto task = std::make_shared<OperatorTask>(get_table);
  _tasks.push_back(task);

  return task;
}

inline std::shared_ptr<OperatorTask> OperatorTranslator::translate(const proto::SortOperator& sort_operator) {
  const auto column_id = ColumnID{static_cast<ColumnID::base_type>(sort_operator.column_id())};
  const auto output_chunk_size = sort_operator.output_chunk_size();
  Assert((sort_operator.has_input_operator()), "Missing Input Operator in Sort.");

  OrderByMode order_by_mode;
  switch (sort_operator.order_by_mode()) {
    case proto::SortOperator::Ascending:
      order_by_mode = OrderByMode::Ascending;
      break;
    case proto::SortOperator::Descending:
      order_by_mode = OrderByMode::Descending;
      break;
    default:
      Fail("Unknown order by mode for operator in operator_translator");
  }

  auto input_task = translate_proto(sort_operator.input_operator());

  auto sort = std::make_shared<Sort>(input_task->get_operator(), column_id, order_by_mode, output_chunk_size);
  auto sort_task = std::make_shared<OperatorTask>(sort);
  input_task->set_as_predecessor_of(sort_task);
  _tasks.push_back(sort_task);

  return sort_task;
}

inline std::shared_ptr<OperatorTask> OperatorTranslator::translate(const proto::UnionAllOperator& union_all_operator) {
  Assert((union_all_operator.has_input_operator1()), "Missing Input Operator 1 in Union All.");
  Assert((union_all_operator.has_input_operator2()), "Missing Input Operator 2 in Union All.");

  auto input_task1 = translate_proto(union_all_operator.input_operator1());
  auto input_task2 = translate_proto(union_all_operator.input_operator2());

  auto union_all = std::make_shared<UnionAll>(input_task1->get_operator(), input_task2->get_operator());
  auto union_all_task = std::make_shared<OperatorTask>(union_all);
  input_task1->set_as_predecessor_of(union_all_task);
  input_task2->set_as_predecessor_of(union_all_task);
  _tasks.push_back(union_all_task);

  return union_all_task;
}

inline std::shared_ptr<OperatorTask> OperatorTranslator::translate(
    const proto::DifferenceOperator& difference_operator) {
  Assert((difference_operator.has_left_operator()), "Missing left Operator in Difference.");
  Assert((difference_operator.has_right_operator()), "Missing right Operator in Difference.");

  auto input_left_task = translate_proto(difference_operator.left_operator());
  auto input_right_task = translate_proto(difference_operator.right_operator());

  auto difference = std::make_shared<Difference>(input_left_task->get_operator(), input_right_task->get_operator());
  auto difference_task = std::make_shared<OperatorTask>(difference);
  input_left_task->set_as_predecessor_of(difference_task);
  input_right_task->set_as_predecessor_of(difference_task);
  _tasks.push_back(difference_task);

  return difference_task;
}

inline std::shared_ptr<OperatorTask> OperatorTranslator::translate(
    const proto::ImportCsvOperator& import_csv_operation) {
  const auto& directory = import_csv_operation.directory();
  const auto& filename = import_csv_operation.filename();
  const auto& full_filename = directory + "/" + filename;

  auto import_csv = std::make_shared<ImportCsv>(full_filename);
  auto import_task = std::make_shared<OperatorTask>(import_csv);
  _tasks.push_back(import_task);

  return import_task;
}

inline std::shared_ptr<OperatorTask> OperatorTranslator::translate(
    const proto::ExportCsvOperator& export_csv_operator) {
  const auto& directory = export_csv_operator.directory();
  const auto& filename = export_csv_operator.filename();
  const auto& full_filename = directory + "/" + filename;
  Assert((export_csv_operator.has_input_operator()), "Missing Input Operator in Export CSV.");

  auto input_task = translate_proto(export_csv_operator.input_operator());
  auto export_csv = std::make_shared<ExportCsv>(input_task->get_operator(), full_filename);
  auto export_csv_task = std::make_shared<OperatorTask>(export_csv);

  input_task->set_as_predecessor_of(export_csv_task);
  _tasks.push_back(export_csv_task);

  return export_csv_task;
}

inline std::shared_ptr<OperatorTask> OperatorTranslator::translate(
    const proto::ExportBinaryOperator& export_binary_operator) {
  const auto& filename = export_binary_operator.filename();
  Assert((export_binary_operator.has_input_operator()), "Missing Input Operator in Export Binary.");

  auto input_task = translate_proto(export_binary_operator.input_operator());
  auto export_binary = std::make_shared<ExportBinary>(input_task->get_operator(), filename);
  auto export_binary_task = std::make_shared<OperatorTask>(export_binary);

  input_task->set_as_predecessor_of(export_binary_task);
  _tasks.push_back(export_binary_task);

  return export_binary_task;
}

inline std::shared_ptr<OperatorTask> OperatorTranslator::translate(const proto::PrintOperator& print_operator) {
  Assert((print_operator.has_input_operator()), "Missing Input Operator in Print.");

  auto input_task = translate_proto(print_operator.input_operator());
  auto print = std::make_shared<Print>(input_task->get_operator());
  auto print_task = std::make_shared<OperatorTask>(print);
  input_task->set_as_predecessor_of(print_task);
  _tasks.push_back(print_task);

  return print_task;
}

const std::vector<std::shared_ptr<OperatorTask>>& OperatorTranslator::build_tasks_from_proto(
    const proto::OperatorVariant& op) {
  translate_proto(op);
  _root_task = _tasks.back();
  return _tasks;
}

// Add a case-statement when adding new operators
std::shared_ptr<OperatorTask> OperatorTranslator::translate_proto(const proto::OperatorVariant& op) {
  switch (op.operator_case()) {
    case proto::OperatorVariant::kGetTable:
      return translate(op.get_table());
    case proto::OperatorVariant::kTableScan:
      return translate(op.table_scan());
    case proto::OperatorVariant::kProjection:
      return translate(op.projection());
    case proto::OperatorVariant::kProduct:
      return translate(op.product());
    case proto::OperatorVariant::kSort:
      return translate(op.sort());
    case proto::OperatorVariant::kUnionAll:
      return translate(op.union_all());
    case proto::OperatorVariant::kImportCsv:
      return translate(op.import_csv());
    case proto::OperatorVariant::kPrint:
      return translate(op.print());
    case proto::OperatorVariant::kDifference:
      return translate(op.difference());
    case proto::OperatorVariant::kExportCsv:
      return translate(op.export_csv());
    case proto::OperatorVariant::kExportBinary:
      return translate(op.export_binary());
    case proto::OperatorVariant::kIndexColumnScan:
      return translate(op.index_column_scan());
    case proto::OperatorVariant::kNestedLoopJoin:
      return translate(op.nested_loop_join());
    case proto::OperatorVariant::OPERATOR_NOT_SET:
      Fail("Operator not set. Missing dependency. Cannot translate proto object to opossum operator.");
    default:
      Fail("Unknown operator type in operator_translator");
  }
  return {};
}

}  // namespace opossum
