#pragma once

#include "storage/index/column_index_type.hpp"
#include "tuning/index/indexable_column_set.hpp"
#include "tuning/tuning_operation.hpp"

namespace opossum {

/**
 * Encapsulates a creation or deletion operation of an index
 */
class IndexTuningOperation : public TuningOperation {
 public:
  IndexTuningOperation(const IndexableColumnSet& column, ColumnIndexType type, bool create_else_delete)
      : _column{column}, _type{type}, _create_else_delete{create_else_delete} {}

  void execute() final;

  void print_on(std::ostream& output) const final;

  /**
   * The column the this operation refers to
   */
  const IndexableColumnSet& column() const;

  /**
   * The type of index that should be created or deleted
   */
  ColumnIndexType type();

  /**
   * true: create index, false: delete index
   */
  bool will_create_else_delete();

 protected:
  IndexableColumnSet _column;
  ColumnIndexType _type;
  bool _create_else_delete;

  void _create_index();
  void _delete_index();
  void _invalidate_cache();
};

}  // namespace opossum
