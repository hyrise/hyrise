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

const std::string Product::name() const { return "Product"; }

std::shared_ptr<const Table> Product::_on_execute() {
  TableCxlumnDefinitions cxlumn_definitions;

  // add columns from left table to output
  for (CxlumnID cxlumn_id{0}; cxlumn_id < input_table_left()->cxlumn_count(); ++cxlumn_id) {
    cxlumn_definitions.emplace_back(input_table_left()->cxlumn_definitions()[cxlumn_id]);
  }

  // add columns from right table to output
  for (CxlumnID cxlumn_id{0}; cxlumn_id < input_table_right()->cxlumn_count(); ++cxlumn_id) {
    cxlumn_definitions.emplace_back(input_table_right()->cxlumn_definitions()[cxlumn_id]);
  }

  auto output = std::make_shared<Table>(cxlumn_definitions, TableType::References);

  for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < input_table_left()->chunk_count(); ++chunk_id_left) {
    for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < input_table_right()->chunk_count(); ++chunk_id_right) {
      _add_product_of_two_chunks(output, chunk_id_left, chunk_id_right);
    }
  }

  return output;
}

void Product::_add_product_of_two_chunks(const std::shared_ptr<Table>& output, ChunkID chunk_id_left,
                                         ChunkID chunk_id_right) {
  const auto chunk_left = input_table_left()->get_chunk(chunk_id_left);
  const auto chunk_right = input_table_right()->get_chunk(chunk_id_right);

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

  std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>> calculated_pos_lists_left;
  std::map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>> calculated_pos_lists_right;

  ChunkSegments output_columns;
  auto is_left_side = true;

  for (const auto& chunk_in : {chunk_left, chunk_right}) {
    // reusing the same code for left and right side - using a reference_wrapper is ugly, but better than code
    // duplication
    auto table = is_left_side ? input_table_left() : input_table_right();

    for (CxlumnID cxlumn_id{0}; cxlumn_id < chunk_in->cxlumn_count(); ++cxlumn_id) {
      std::shared_ptr<const Table> referenced_table;
      CxlumnID referenced_column;
      std::shared_ptr<const PosList> pos_list_in;

      if (auto ref_col_in = std::dynamic_pointer_cast<const ReferenceSegment>(chunk_in->get_column(cxlumn_id))) {
        referenced_table = ref_col_in->referenced_table();
        referenced_column = ref_col_in->referenced_cxlumn_id();
        pos_list_in = ref_col_in->pos_list();
      } else {
        referenced_table = is_left_side ? input_table_left() : input_table_right();
        referenced_column = cxlumn_id;
      }

      // see if we can reuse a PosList that we already calculated - important to use a reference here so that the map
      // gets updated accordingly
      auto& pos_list_out = (is_left_side ? calculated_pos_lists_left : calculated_pos_lists_right)[pos_list_in];
      if (!pos_list_out) {
        // can't reuse
        pos_list_out = std::make_shared<PosList>();
        pos_list_out->reserve(chunk_left->size() * chunk_right->size());
        for (size_t i = 0; i < chunk_left->size() * chunk_right->size(); ++i) {
          // size_t is sufficient here, because ChunkOffset::max is 2^32 and (2^32 * 2^32 = 2^64)
          ChunkOffset offset = is_left_side ? (i / chunk_right->size()) : (i % chunk_right->size());
          if (pos_list_in) {
            pos_list_out->emplace_back((*pos_list_in)[offset]);
          } else {
            pos_list_out->emplace_back(RowID{is_left_side ? chunk_id_left : chunk_id_right, offset});
          }
        }
      }
      output_columns.push_back(std::make_shared<ReferenceSegment>(referenced_table, referenced_column, pos_list_out));
    }

    is_left_side = false;
  }

  output->append_chunk(output_columns);
}
std::shared_ptr<AbstractOperator> Product::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Product>(copied_input_left, copied_input_right);
}

void Product::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
