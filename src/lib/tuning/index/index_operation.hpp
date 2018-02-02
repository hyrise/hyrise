#pragma once

#include "storage/index/column_index_type.hpp"
#include "tuning/index/column_ref.hpp"
#include "tuning/tuning_operation.hpp"

namespace opossum {

/**
 * Encapsulates a creation or deletion operation of an index
 */
class IndexOperation : public TuningOperation {
 public:
  IndexOperation(const ColumnRef& column, ColumnIndexType type, bool create)
      : column{column}, type{type}, create{create} {}

  void execute() final;

  void print_on(std::ostream& output) const final;

  /**
   * The column the this operation refers to
   */
  ColumnRef column;

  /**
   * The type of index that should be created or deleted
   */
  ColumnIndexType type;

  /**
   * true: create index, false: delete index
   */
  bool create;

 protected:
  void _create_index();
  void _delete_index();
};

}  // namespace opossum
