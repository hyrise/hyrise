#include "join_nested_loop_a.hpp"

#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "product.hpp"

#include "storage/base_attribute_vector.hpp"
#include "storage/column_visitable.hpp"

#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

JoinNestedLoopA::JoinNestedLoopA(const std::shared_ptr<const AbstractOperator> left,
                                 const std::shared_ptr<const AbstractOperator> right, const JoinMode mode,
                                 const std::pair<ColumnID, ColumnID> &column_ids, const ScanType scan_type)
    : AbstractJoinOperator(left, right, mode, column_ids, scan_type) {}

const std::string JoinNestedLoopA::name() const { return "JoinNestedLoopA"; }

uint8_t JoinNestedLoopA::num_in_tables() const { return 2; }

uint8_t JoinNestedLoopA::num_out_tables() const { return 1; }

std::shared_ptr<AbstractOperator> JoinNestedLoopA::recreate(const std::vector<AllParameterVariant> &args) const {
  return std::make_shared<JoinNestedLoopA>(_input_left->recreate(args), _input_right->recreate(args), _mode,
                                           _column_ids, _scan_type);
}

std::shared_ptr<const Table> JoinNestedLoopA::_on_execute() {
  const auto &type_left = _input_table_left()->column_type(_column_ids.first);
  const auto &type_right = _input_table_right()->column_type(_column_ids.second);
  _impl = make_unique_by_column_types<AbstractReadOnlyOperatorImpl, JoinNestedLoopAImpl>(
      type_left, type_right, _input_left, _input_right, _mode, _column_ids, _scan_type);
  return _impl->_on_execute();
}

void JoinNestedLoopA::_on_cleanup() { _impl.reset(); }

// We need to use the impl pattern because the join operator depends on the type of the columns
template <typename LeftType, typename RightType>
class JoinNestedLoopA::JoinNestedLoopAImpl : public AbstractJoinOperatorImpl {
 public:
  JoinNestedLoopAImpl(const std::shared_ptr<const AbstractOperator> left,
                      const std::shared_ptr<const AbstractOperator> right, const JoinMode mode,
                      const std::pair<ColumnID, ColumnID> &column_ids, const ScanType scan_type)
      : _left_in_table(left->get_output()),
        _right_in_table(right->get_output()),
        _mode(mode),
        _left_column_id(column_ids.first),
        _right_column_id(column_ids.second),
        _scan_type(scan_type),
        _output_table(std::make_shared<Table>()) {
    // Parsing the join operator
    switch (_scan_type) {
      case ScanType::OpEquals: {
        _comparator = [](LeftType left_val, RightType right_val) { return value_equal(left_val, right_val); };
        break;
      }
      case ScanType::OpNotEquals: {
        _comparator = [](LeftType left_val, RightType right_val) { return !value_equal(left_val, right_val); };
        break;
      }
      case ScanType::OpLessThan: {
        _comparator = [](LeftType left_val, RightType right_val) { return value_smaller(left_val, right_val); };
        break;
      }
      case ScanType::OpLessThanEquals: {
        _comparator = [](LeftType left_val, RightType right_val) { return !value_greater(left_val, right_val); };
        break;
      }
      case ScanType::OpGreaterThan: {
        _comparator = [](LeftType left_val, RightType right_val) { return value_greater(left_val, right_val); };
        break;
      }
      case ScanType::OpGreaterThanEquals: {
        _comparator = [](LeftType left_val, RightType right_val) { return !value_smaller(left_val, right_val); };
        break;
      }
      default:
        Fail(std::string("Unsupported operator for join."));
    }
  }

  virtual ~JoinNestedLoopAImpl() = default;
  /*
  We need to use the Visitor Pattern to identify column types. We therefor store information about the join in this
  context. Below we have two childs of JoinNestedLoopAContext for BuilderLeft and BuilderRight.
  Both have a common constructor interface, but differ in the way they initialize their members.
  */
  struct JoinNestedLoopAContext : ColumnVisitableContext {
    JoinNestedLoopAContext() {}

    JoinNestedLoopAContext(std::shared_ptr<BaseColumn> coleft, std::shared_ptr<BaseColumn> coright, ChunkID left_id,
                           ChunkID right_id, std::shared_ptr<PosList> left, std::shared_ptr<PosList> right,
                           JoinMode mode, std::function<bool(LeftType, RightType)> compare,
                           std::shared_ptr<std::map<RowID, bool>> null_value_rows_left,
                           std::shared_ptr<std::map<RowID, bool>> null_value_rows_right,
                           std::shared_ptr<std::vector<ChunkOffset>> filter_left = nullptr,
                           std::shared_ptr<std::vector<ChunkOffset>> filter_right = nullptr)
        : column_left(coleft),
          column_right(coright),
          chunk_id_left(left_id),
          chunk_id_right(right_id),
          pos_list_left(left),
          pos_list_right(right),
          join_mode(mode),
          compare_func(compare),
          chunk_offsets_in_left(filter_left),
          chunk_offsets_in_right(filter_right),
          rows_potentially_joined_with_null_values_left(null_value_rows_left),
          rows_potentially_joined_with_null_values_right(null_value_rows_right) {}

    std::shared_ptr<BaseColumn> column_left;
    std::shared_ptr<BaseColumn> column_right;
    ChunkID chunk_id_left;
    ChunkID chunk_id_right;
    std::shared_ptr<PosList> pos_list_left;
    std::shared_ptr<PosList> pos_list_right;
    JoinMode join_mode;
    std::function<bool(LeftType, RightType)> compare_func;
    size_t size_left;
    std::function<LeftType(ChunkOffset)> get_left_column_value;
    std::shared_ptr<std::vector<ChunkOffset>> chunk_offsets_in_left;
    std::shared_ptr<std::vector<ChunkOffset>> chunk_offsets_in_right;
    std::shared_ptr<std::map<RowID, bool>> rows_potentially_joined_with_null_values_left;
    std::shared_ptr<std::map<RowID, bool>> rows_potentially_joined_with_null_values_right;
  };

  // separate constructor for use in ReferenceColumn::visit_dereferenced
  struct JoinNestedLoopALeftContext : public JoinNestedLoopAContext {
    JoinNestedLoopALeftContext(std::shared_ptr<BaseColumn> referenced_column, const std::shared_ptr<const Table>,
                               std::shared_ptr<ColumnVisitableContext> base_context, ChunkID chunk_id,
                               std::shared_ptr<std::vector<ChunkOffset>> chunk_offsets) {
      auto ctx = std::static_pointer_cast<JoinNestedLoopAContext>(base_context);

      this->column_left = referenced_column;
      this->column_right = ctx->column_right;
      this->chunk_id_left = chunk_id;
      this->chunk_id_right = ctx->chunk_id_right;
      this->pos_list_left = ctx->pos_list_left;
      this->pos_list_right = ctx->pos_list_right;
      this->join_mode = ctx->join_mode;
      this->compare_func = ctx->compare_func;
      this->size_left = ctx->size_left;
      this->get_left_column_value = ctx->get_left_column_value;
      this->rows_potentially_joined_with_null_values_left = ctx->rows_potentially_joined_with_null_values_left;
      this->rows_potentially_joined_with_null_values_right = ctx->rows_potentially_joined_with_null_values_right;
      this->chunk_offsets_in_left = chunk_offsets;
    }
  };

  // separate constructor for use in ReferenceColumn::visit_dereferenced
  struct JoinNestedLoopARightContext : public JoinNestedLoopAContext {
    JoinNestedLoopARightContext(std::shared_ptr<BaseColumn> referenced_column, const std::shared_ptr<const Table>,
                                std::shared_ptr<ColumnVisitableContext> base_context, ChunkID chunk_id,
                                std::shared_ptr<std::vector<ChunkOffset>> chunk_offsets) {
      auto ctx = std::static_pointer_cast<JoinNestedLoopAContext>(base_context);

      this->column_left = ctx->column_left;
      this->column_right = referenced_column;
      this->chunk_id_left = ctx->chunk_id_left;
      this->chunk_id_right = chunk_id;
      this->pos_list_left = ctx->pos_list_left;
      this->pos_list_right = ctx->pos_list_right;
      this->join_mode = ctx->join_mode;
      this->compare_func = ctx->compare_func;
      this->size_left = ctx->size_left;
      this->get_left_column_value = ctx->get_left_column_value;
      this->rows_potentially_joined_with_null_values_left = ctx->rows_potentially_joined_with_null_values_left;
      this->rows_potentially_joined_with_null_values_right = ctx->rows_potentially_joined_with_null_values_right;
      this->chunk_offsets_in_left = ctx->chunk_offsets_in_left;
      this->chunk_offsets_in_right = chunk_offsets;
    }
  };

  /*
  Double Visitor approach

  The implementation of the join operation depends on the column type of both join columns. For now OpossumDB supports 3
  column types, namely ValueColumn, DictionaryColumn, and ReferenceColumn.
  We need to cover all the combinations of these types in the following section.

  The following approach works as follows:
  By using the ColumnVisitable interface we first determine the column type of the left column. The respective handle_*
  function in BuilderLeft will be called. Based on the type we define the 'value-access'-function.
  Following the visitor for column_right, we figure out the type of the right column. Again the respective handle_*
  function in BuilderRight will be called.
  Since we now know how to access values from both columns, we can perform the actual join.
  */
  struct BuilderRight : public ColumnVisitable {
    void handle_value_column(BaseColumn &, std::shared_ptr<ColumnVisitableContext> context) override {
      auto ctx = std::static_pointer_cast<JoinNestedLoopAContext>(context);

      auto vc_right = std::static_pointer_cast<ValueColumn<RightType>>(ctx->column_right);
      const auto &right_values = vc_right->values();
      // Function to get the value of right column
      auto get_right_column_value = [&right_values](ChunkOffset index) { return right_values[index]; };

      perform_join(ctx->get_left_column_value, get_right_column_value, ctx, ctx->size_left, right_values.size());
    }

    void handle_dictionary_column(BaseColumn &, std::shared_ptr<ColumnVisitableContext> context) override {
      auto ctx = std::static_pointer_cast<JoinNestedLoopAContext>(context);

      auto dc_right = std::static_pointer_cast<DictionaryColumn<RightType>>(ctx->column_right);
      const auto &right_dictionary = static_cast<const pmr_vector<RightType> &>(*dc_right->dictionary());
      const auto &right_attribute_vector = static_cast<const BaseAttributeVector &>(*dc_right->attribute_vector());

      // Function to get the value of right column
      auto get_right_column_value = [&right_dictionary, &right_attribute_vector](ChunkOffset index) {
        return right_dictionary[right_attribute_vector.get(index)];
      };

      perform_join(ctx->get_left_column_value, get_right_column_value, ctx, ctx->size_left,
                   right_attribute_vector.size());
    }

    void handle_reference_column(ReferenceColumn &ref_column,
                                 std::shared_ptr<ColumnVisitableContext> context) override {
      ref_column.visit_dereferenced<JoinNestedLoopARightContext>(*this, context);
    }
  };

  struct BuilderLeft : public ColumnVisitable {
    void handle_value_column(BaseColumn &, std::shared_ptr<ColumnVisitableContext> context) override {
      auto ctx = std::static_pointer_cast<JoinNestedLoopAContext>(context);
      auto vc_left = std::static_pointer_cast<ValueColumn<LeftType>>(ctx->column_left);
      const auto &left_values = vc_left->values();

      // Size of the current left column
      ctx->size_left = left_values.size();
      // Function to get the value of left column
      ctx->get_left_column_value = [&left_values](ChunkOffset index) { return left_values[index]; };

      BuilderRight builder_right;
      ctx->column_right->visit(builder_right, context);
    }

    void handle_dictionary_column(BaseColumn &, std::shared_ptr<ColumnVisitableContext> context) override {
      auto ctx = std::static_pointer_cast<JoinNestedLoopAContext>(context);
      auto dc_left = std::static_pointer_cast<DictionaryColumn<LeftType>>(ctx->column_left);

      const auto &left_dictionary = static_cast<const pmr_vector<LeftType> &>(*dc_left->dictionary());
      const auto &left_attribute_vector = static_cast<const BaseAttributeVector &>(*dc_left->attribute_vector());

      // Size of the current left column
      ctx->size_left = left_attribute_vector.size();
      // Function to get the value of left column
      ctx->get_left_column_value = [&left_dictionary, &left_attribute_vector](ChunkOffset index) {
        return left_dictionary[left_attribute_vector.get(index)];
      };

      BuilderRight builder_right;
      ctx->column_right->visit(builder_right, context);
    }

    void handle_reference_column(ReferenceColumn &ref_column,
                                 std::shared_ptr<ColumnVisitableContext> context) override {
      ref_column.visit_dereferenced<JoinNestedLoopALeftContext>(*this, context);
    }
  };

  static void perform_join(std::function<LeftType(ChunkOffset)> get_left_column_value,
                           std::function<RightType(ChunkOffset)> get_right_column_value,
                           std::shared_ptr<JoinNestedLoopAContext> context, const size_t size_left,
                           const size_t size_right) {
    auto &unmatched_rows_map_left = context->rows_potentially_joined_with_null_values_left;
    auto &unmatched_rows_map_right = context->rows_potentially_joined_with_null_values_right;
    if (context->chunk_offsets_in_left || context->chunk_offsets_in_right) {
      // This ValueColumn is referenced by a ReferenceColumn (i.e., is probably filtered). We only return the matching
      // rows within the filtered column, together with their original position

      auto chunk_offsets_in_left = context->chunk_offsets_in_left;
      auto chunk_offsets_in_right = context->chunk_offsets_in_right;

      if (!chunk_offsets_in_left) {
        chunk_offsets_in_left = std::make_shared<std::vector<ChunkOffset>>(size_left);
        std::iota(chunk_offsets_in_left->begin(), chunk_offsets_in_left->end(), 0);
      } else if (!chunk_offsets_in_right) {
        chunk_offsets_in_right = std::make_shared<std::vector<ChunkOffset>>(size_right);
        std::iota(chunk_offsets_in_right->begin(), chunk_offsets_in_right->end(), 0);
      }

      ChunkOffset row_left = 0;
      for (const ChunkOffset &offset_in_left_value_column : *(chunk_offsets_in_left)) {
        ChunkOffset row_right = 0;
        for (const ChunkOffset &offset_in_right_value_column : *(chunk_offsets_in_right)) {
          auto is_match = context->compare_func(get_left_column_value(offset_in_left_value_column),
                                                get_right_column_value(offset_in_right_value_column));

          auto current_right = RowID{context->chunk_id_right, row_right};
          auto current_left = RowID{context->chunk_id_left, row_left};
          if (is_match) {
            // For outer joins we need to mark these rows, since they don't need to be added later on.
            if (context->join_mode == JoinMode::Right || context->join_mode == JoinMode::Outer) {
              (*unmatched_rows_map_right)[current_right] = false;
            }
            if (context->join_mode == JoinMode::Left || context->join_mode == JoinMode::Outer) {
              (*unmatched_rows_map_left)[current_left] = false;
            }
            write_poslists(context, row_left, row_right);
          } else {
            // If this row combination has been joined previously, don't do anything.
            // If they are not in the unmatched_rows_map, add them here.
            if (context->join_mode == JoinMode::Right || context->join_mode == JoinMode::Outer) {
              if (unmatched_rows_map_right->find(current_right) == unmatched_rows_map_right->end()) {
                (*unmatched_rows_map_right)[current_right] = true;
              }
            }
            if (context->join_mode == JoinMode::Left || context->join_mode == JoinMode::Outer) {
              if (unmatched_rows_map_left->find(current_left) == unmatched_rows_map_left->end()) {
                (*unmatched_rows_map_left)[current_left] = true;
              }
            }
          }
          row_right++;
        }
        row_left++;
      }
    } else {
      for (ChunkOffset row_left = 0; row_left < size_left; ++row_left) {
        for (ChunkOffset row_right = 0; row_right < size_right; ++row_right) {
          auto is_match = context->compare_func(get_left_column_value(row_left), get_right_column_value(row_right));

          auto current_right = RowID{context->chunk_id_right, row_right};
          auto current_left = RowID{context->chunk_id_left, row_left};
          if (is_match) {
            // For outer joins we need to mark these rows, since they don't need to be added later on.
            if (context->join_mode == JoinMode::Right || context->join_mode == JoinMode::Outer) {
              (*unmatched_rows_map_right)[current_right] = false;
            }
            if (context->join_mode == JoinMode::Left || context->join_mode == JoinMode::Outer) {
              (*unmatched_rows_map_left)[current_left] = false;
            }
            write_poslists(context, row_left, row_right);
          } else {
            // If this row combination has been joined previously, don't do anything.
            // If they are not in the unmatched_rows_map, add them here.
            if (context->join_mode == JoinMode::Right || context->join_mode == JoinMode::Outer) {
              if (unmatched_rows_map_right->find(current_right) == unmatched_rows_map_right->end()) {
                (*unmatched_rows_map_right)[current_right] = true;
              }
            }
            if (context->join_mode == JoinMode::Left || context->join_mode == JoinMode::Outer) {
              if (unmatched_rows_map_left->find(current_left) == unmatched_rows_map_left->end()) {
                (*unmatched_rows_map_left)[current_left] = true;
              }
            }
          }
        }
      }
    }
  }

  std::shared_ptr<const Table> _on_execute() override {
    // Preparing output table by adding columns from left table

    for (ColumnID column_id{0}; column_id < _left_in_table->col_count(); ++column_id) {
      _output_table->add_column_definition(_left_in_table->column_name(column_id),
                                           _left_in_table->column_type(column_id), true);
    }

    // Preparing output table by adding columns from right table
    for (ColumnID column_id{0}; column_id < _right_in_table->col_count(); ++column_id) {
      _output_table->add_column_definition(_right_in_table->column_name(column_id),
                                           _right_in_table->column_type(column_id), true);
    }

    /*
    We need a global map to store information about rows that are matched, resp. unmatched. This is used to implement
    outer joins. After iterating through all chunks and checking for possible matches, we are going to
    create additional output rows for the missing rows in either the Left or the Right table. A boolean value of true
    indicates those rows that need to be joined with null values.
    */
    auto rows_potentially_joined_with_null_values_left = std::make_shared<std::map<RowID, bool>>();
    auto rows_potentially_joined_with_null_values_right = std::make_shared<std::map<RowID, bool>>();

    // Scan all chunks from left input
    for (ChunkID chunk_id_left = ChunkID{0}; chunk_id_left < _left_in_table->chunk_count(); ++chunk_id_left) {
      auto column_left = _left_in_table->get_chunk(chunk_id_left).get_column(_left_column_id);

      BuilderLeft builder_left;

      // Scan all chunks for right input
      for (ChunkID chunk_id_right = ChunkID{0}; chunk_id_right < _right_in_table->chunk_count(); ++chunk_id_right) {
        auto column_right = _right_in_table->get_chunk(chunk_id_right).get_column(_right_column_id);

        auto pos_list_left = std::make_shared<PosList>();
        auto pos_list_right = std::make_shared<PosList>();

        auto context = std::make_shared<JoinNestedLoopAContext>(
            column_left, column_right, chunk_id_left, chunk_id_right, pos_list_left, pos_list_right, _mode, _comparator,
            rows_potentially_joined_with_null_values_left, rows_potentially_joined_with_null_values_right);

        // Use double visitor to join columns
        column_left->visit(builder_left, context);

        // Different length of poslists would lead to corrupt output chunk.
        DebugAssert((pos_list_left->size() == pos_list_right->size()),
                    "JoinNestedLoopA did generate different number of outputs for Left and Right.");

        // Skip Chunks without match
        if (pos_list_left->size() == 0) {
          continue;
        }

        auto output_chunk = Chunk();

        // Add columns from left table to output chunk
        write_output_chunks(output_chunk, _left_in_table, chunk_id_left, pos_list_left);

        // Add columns from right table to output chunk
        write_output_chunks(output_chunk, _right_in_table, chunk_id_right, pos_list_right);

        _output_table->add_chunk(std::move(output_chunk));
      }
    }

    /*
    We now have checked all row combinations for possible matches. Let's add the missing rows from Left or Right input
    for Left Outer and Right Outer Joins.
    The map that we are iterating through contains one pair for all rows from either Left or Right indicating whether
    a join with a NULL value is necessary.

    We are going to create a new Chunk for each of these rows, because this is the simpliest solution. The difficulty
    lies in resolving chunks with reference columns. We would need to somehow split the remaining rows into groups of
    reference columns and value/dictionary columns rows.
    An improvement would be to group the missing rows by chunk_id and create a new Chunk per group.
    */
    if (_mode == JoinMode::Left || _mode == JoinMode::Outer) {
      for (const auto &elem : *rows_potentially_joined_with_null_values_left) {
        if (elem.second) {
          auto pos_list_left = std::make_shared<PosList>();
          auto pos_list_right = std::make_shared<PosList>();

          pos_list_left->emplace_back(elem.first);
          pos_list_right->emplace_back(NULL_ROW_ID);

          auto output_chunk = Chunk();

          write_output_chunks(output_chunk, _left_in_table, elem.first.chunk_id, pos_list_left, false);
          write_output_chunks(output_chunk, _right_in_table, elem.first.chunk_id, pos_list_right, true);

          _output_table->add_chunk(std::move(output_chunk));
        }
      }
    }
    if (_mode == JoinMode::Right || _mode == JoinMode::Outer) {
      for (const auto &elem : *rows_potentially_joined_with_null_values_right) {
        if (elem.second) {
          auto pos_list_left = std::make_shared<PosList>();
          auto pos_list_right = std::make_shared<PosList>();

          pos_list_left->emplace_back(NULL_ROW_ID);
          pos_list_right->emplace_back(elem.first);

          auto output_chunk = Chunk();

          write_output_chunks(output_chunk, _left_in_table, elem.first.chunk_id, pos_list_left, true);
          write_output_chunks(output_chunk, _right_in_table, elem.first.chunk_id, pos_list_right, false);

          _output_table->add_chunk(std::move(output_chunk));
        }
      }
    }

    return _output_table;
  }

  /*
  This method writes the actual output chunks for either the Left or the Right side. It'll write ReferenceColumns for
  all the columns of the input table. The way we currently handle null values forces us to pass in whether there are
  null values in this poslist:
  When we write the null values for (Left/Right) Outer Joins we simply use a ChunkID=0, which does not necessarily need
  to exist. Thus we cannot be sure to find an actual column in the input table for that ChunkID.
  Additionally, we think that the implementation of null values is not final yet and a proper implementation of null
  values might require changes here.
  */
  static void write_output_chunks(Chunk &output_chunk, const std::shared_ptr<const Table> input_table, ChunkID chunk_id,
                                  std::shared_ptr<PosList> pos_list, bool null_value = false) {
    // Add columns from left table to output chunk
    for (ColumnID column_id{0}; column_id < input_table->col_count(); ++column_id) {
      std::shared_ptr<BaseColumn> column;

      // Keep it simple for now and handle null_values seperately. We don't have a chunk_id for null values and thus
      // don't want to risk finding a ReferenceColumn for a random chunk_id.
      if (null_value) {
        column = std::make_shared<ReferenceColumn>(input_table, column_id, pos_list);
      } else {
        DebugAssert(chunk_id < input_table->chunk_count(), "Chunk id out of range");
        if (auto ref_col_left =
                std::dynamic_pointer_cast<ReferenceColumn>(input_table->get_chunk(chunk_id).get_column(column_id))) {
          auto new_pos_list = std::make_shared<PosList>();

          // de-reference to the correct RowID so the output can be used in a Multi Join
          for (const auto &row : *pos_list) {
            new_pos_list->push_back(ref_col_left->pos_list()->at(row.chunk_offset));
          }

          column = std::make_shared<ReferenceColumn>(ref_col_left->referenced_table(),
                                                     ref_col_left->referenced_column_id(), new_pos_list);
        } else {
          column = std::make_shared<ReferenceColumn>(input_table, column_id, pos_list);
        }
      }

      output_chunk.add_column(column);
    }
  }

  static void write_poslists(std::shared_ptr<JoinNestedLoopAContext> context, ChunkOffset row_left,
                             ChunkOffset row_right) {
    const auto left_chunk_id = context->chunk_id_left;
    const auto right_chunk_id = context->chunk_id_right;

    context->pos_list_left->emplace_back(RowID{left_chunk_id, row_left});
    context->pos_list_right->emplace_back(RowID{right_chunk_id, row_right});
  }

 protected:
  const std::shared_ptr<const Table> _left_in_table, _right_in_table;
  const JoinMode _mode;
  const ColumnID _left_column_id, _right_column_id;
  const ScanType _scan_type;
  const std::shared_ptr<Table> _output_table;
  std::function<bool(LeftType, RightType)> _comparator;
};

}  // namespace opossum
