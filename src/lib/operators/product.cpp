#include "product.hpp"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/reference_segment.hpp"

namespace opossum {
Product::Product(const std::shared_ptr<const AbstractOperator>& left,
                 const std::shared_ptr<const AbstractOperator>& right)
    : AbstractReadOnlyOperator(OperatorType::Product, left, right) {}

const std::string& Product::name() const {
  static const auto name = std::string{"Product"};
  return name;
}

std::shared_ptr<const Table> Product::_on_execute() {
  TableColumnDefinitions column_definitions;

  // add columns from left table to output
  for (ColumnID column_id{0}; column_id < left_input_table()->column_count(); ++column_id) {
    column_definitions.emplace_back(left_input_table()->column_definitions()[column_id]);
  }

  // add columns from right table to output
  for (ColumnID column_id{0}; column_id < right_input_table()->column_count(); ++column_id) {
    column_definitions.emplace_back(right_input_table()->column_definitions()[column_id]);
  }

  auto output = std::make_shared<Table>(column_definitions, TableType::References);
  auto chunk_count_left_table = left_input_table()->chunk_count();
  auto chunk_count_right_table = right_input_table()->chunk_count();

  for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < chunk_count_left_table; ++chunk_id_left) {
    const auto chunk_left = left_input_table()->get_chunk(chunk_id_left);
    Assert(chunk_left, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < chunk_count_right_table; ++chunk_id_right) {
      const auto chunk_right = right_input_table()->get_chunk(chunk_id_right);
      Assert(chunk_right, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

      _add_product_of_two_chunks(output, chunk_id_left, chunk_id_right);
    }
  }

  return output;
}

void Product::_add_product_of_two_chunks(const std::shared_ptr<Table>& output, ChunkID chunk_id_left,
                                         ChunkID chunk_id_right) {
  const auto chunk_left = left_input_table()->get_chunk(chunk_id_left);
  const auto chunk_right = right_input_table()->get_chunk(chunk_id_right);

  // we use an approach here in which we do not have nested loops for left and right but create both sides separately
  // When the result looks like this:
  //   l1 r1
  //   l1 r2
  //   l1 r3
  //   l2 r1
  //   l2 r2
  //   l2 r3
  // we can first repeat each line on the left side #rightSide times and then repeat the ascending sequence for the
  // right side #leftSide times

  std::map<std::shared_ptr<const AbstractPosList>, std::shared_ptr<RowIDPosList>> calculated_pos_lists_left;
  std::map<std::shared_ptr<const AbstractPosList>, std::shared_ptr<RowIDPosList>> calculated_pos_lists_right;

  Segments output_segments;
  auto is_left_side = true;

  for (const auto& chunk_in : {chunk_left, chunk_right}) {
    // reusing the same code for left and right side - using a reference_wrapper is ugly, but better than code
    // duplication
    auto table = is_left_side ? left_input_table() : right_input_table();

    for (ColumnID column_id{0}; column_id < chunk_in->column_count(); ++column_id) {
      std::shared_ptr<const Table> referenced_table;
      ColumnID referenced_segment;
      std::shared_ptr<const AbstractPosList> pos_list_in;

      if (auto reference_segment_in =
              std::dynamic_pointer_cast<const ReferenceSegment>(chunk_in->get_segment(column_id))) {
        referenced_table = reference_segment_in->referenced_table();
        referenced_segment = reference_segment_in->referenced_column_id();
        pos_list_in = reference_segment_in->pos_list();
      } else {
        referenced_table = is_left_side ? left_input_table() : right_input_table();
        referenced_segment = column_id;
      }

      // see if we can reuse a PosList that we already calculated - important to use a reference here so that the map
      // gets updated accordingly
      auto& pos_list_out = (is_left_side ? calculated_pos_lists_left : calculated_pos_lists_right)[pos_list_in];
      if (!pos_list_out) {
        // can't reuse
        pos_list_out = std::make_shared<RowIDPosList>();
        pos_list_out->reserve(chunk_left->size() * chunk_right->size());
        for (size_t i = 0; i < chunk_left->size() * chunk_right->size(); ++i) {
          // size_t is sufficient here, because ChunkOffset::max is 2^32 and (2^32 * 2^32 = 2^64)
          auto offset = is_left_side ? static_cast<ChunkOffset>(i / chunk_right->size())
                                     : static_cast<ChunkOffset>(i % chunk_right->size());
          if (pos_list_in) {
            pos_list_out->emplace_back((*pos_list_in)[offset]);
          } else {
            pos_list_out->emplace_back(RowID{is_left_side ? chunk_id_left : chunk_id_right, offset});
          }
        }
      }
      output_segments.push_back(std::make_shared<ReferenceSegment>(referenced_table, referenced_segment, pos_list_out));
    }

    is_left_side = false;
  }

  output->append_chunk(output_segments);
}

std::shared_ptr<AbstractOperator> Product::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<Product>(copied_left_input, copied_right_input);
}

void Product::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
