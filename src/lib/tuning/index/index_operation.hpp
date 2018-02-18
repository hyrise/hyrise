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
      : _column{column}, _type{type}, _create{create} {}

  void execute() final;

  void print_on(std::ostream& output) const final;

  /**
   * The column the this operation refers to
   */
  const ColumnRef& column() const;

  /**
   * The type of index that should be created or deleted
   */
  ColumnIndexType type();

  /**
   * true: create index, false: delete index
   */
  bool create();

 protected:
  ColumnRef _column;
  ColumnIndexType _type;
  bool _create;

  void _create_index();
  void _delete_index();
  void _invalidate_cache();
};

}  // namespace opossum
