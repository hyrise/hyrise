#pragma once

#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>

#include "base_table_scan_impl.hpp"

#include "storage/column_iterables.hpp"
#include "storage/column_iterables/chunk_offset_mapping.hpp"
#include "storage/column_visitable.hpp"

#include "types.hpp"

namespace opossum {

class Table;
class ReferenceColumn;
class AttributeVectorIterable;
class DeprecatedAttributeVectorIterable;

/**
 * @brief The base class of table scan impls that scan a single column
 *
 * Resolves reference columns. The position list of reference columns
 * is split by the referenced columns and then each is visited separately.
 */
class BaseSingleColumnTableScanImpl : public BaseTableScanImpl, public ColumnVisitable {
 public:
  BaseSingleColumnTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                                const PredicateCondition predicate_condition);

  PosList scan_chunk(ChunkID chunk_id) override;

  void handle_column(const ReferenceColumn& left_column, std::shared_ptr<ColumnVisitableContext> base_context) override;

 protected:
  /**
   * @brief the context used for the columns’ visitor pattern
   */
  struct Context : public ColumnVisitableContext {
    Context(const ChunkID chunk_id, PosList& matches_out, std::optional<ColumnPointAccessPlan> access_plan)
        : _chunk_id{chunk_id}, _matches_out{matches_out}, _access_plan{std::move(access_plan)} {}

    Context(const ChunkID chunk_id, PosList& matches_out) : Context{chunk_id, matches_out, std::nullopt} {}

    // Copy everything from other except mapped_chunk_offsets
    Context(Context& other, const ColumnPointAccessPlan& access_plan)
        : Context{other._chunk_id, other._matches_out, std::move(access_plan)} {}

    const ChunkID _chunk_id;
    PosList& _matches_out;

    std::optional<ColumnPointAccessPlan> _access_plan;
  };

  /**
   * @defgroup Create attribute vector iterable from dictionary column
   *
   * Only needed as long as there are two dictionary column implementations
   *
   * @{
   */

  static AttributeVectorIterable _create_attribute_vector_iterable(const BaseDictionaryColumn& column);

  static DeprecatedAttributeVectorIterable _create_attribute_vector_iterable(
      const BaseDeprecatedDictionaryColumn& column);

  /**@}*/

  void _visit_referenced(const ReferenceColumn& left_column, const ChunkID referenced_chunk_id, Context& context,
                         ColumnPointAccessPlan access_plan);
};

}  // namespace opossum
