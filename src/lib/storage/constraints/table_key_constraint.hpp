#pragma once

#include <set>
#include <unordered_set>
#include <utility>

#include <oneapi/tbb/concurrent_unordered_set.h>  // NOLINT(build/include_order): cpplint identifies TBB as C system headers.

#include "abstract_table_constraint.hpp"
#include "types.hpp"

namespace hyrise {

// NOLINTNEXTLINE(readability-identifier-naming)
enum class KeyConstraintType : uint8_t { PRIMARY_KEY, UNIQUE };

enum class ValidationResultType : uint8_t { VALID, INVALID };

/**
 * Container class to define uniqueness constraints for tables. As defined by SQL, two types of keys are supported:
 * PRIMARY KEY and UNIQUE keys. Table key constraints are translated to unique column combinations (UCCs) in the LQP.
 */
class TableKeyConstraint final : public AbstractTableConstraint {
 public:
  ~TableKeyConstraint() override = default;
  /**
   * By requesting a std::set for the constructor, we ensure that the ColumnIDs have a well-defined order when creating
   * the vector (and equal constraints have equal columns). Thus, we can safely hash and compare key constraints without
   * violating the set semantics of the constraint.
   */
  TableKeyConstraint(std::set<ColumnID>&& columns, const KeyConstraintType key_type,
                     const CommitID last_validated_on = MAX_COMMIT_ID, const CommitID last_invalidated = MAX_COMMIT_ID);
  TableKeyConstraint() = delete;

  TableKeyConstraint(const TableKeyConstraint& other);
  TableKeyConstraint& operator=(const TableKeyConstraint& other);

  TableKeyConstraint(TableKeyConstraint&& other) noexcept;
  TableKeyConstraint& operator=(TableKeyConstraint&& other) noexcept;

  const std::set<ColumnID>& columns() const;

  KeyConstraintType key_type() const;

  CommitID last_validated_on() const;
  CommitID last_invalidated_on() const;

  /**
   * Returns whether or not this key constraint can become invalid if the table data changes. This is false for constraints specified
   * by the table schema, but true for the "spurious" uniqueness of columns in any table state as adding duplicates
   * would make them no longer unique.
   */
  bool can_become_invalid() const;

  /**
   * Returns the result of the last validation attempt in the UCC discovery plugin. This does not necessarily mean that
   * the constraint is valid for the current state of the table; it only means that it was validated last. To check if
   * a constraint is guaranteed to be valid on a given table, use `key_constraint_is_confidently_valid`.
   */
  ValidationResultType last_validation_result() const;
  void revalidated_on(const CommitID revalidation_commit_id) const;
  void invalidated_on(const CommitID invalidation_commit_id) const;

  size_t hash() const override;

  /**
   * Required for storing TableKeyConstraints in a sort-based std::set, which we use for formatted printing of the
   * entire table's unique constraints. The comparison result does not need to be meaningful as long as it is
   * consistent. In fact, we favor primary key constraints to be printed first.
   */
  bool operator<(const TableKeyConstraint& rhs) const;

 protected:
  bool _on_equals(const AbstractTableConstraint& table_constraint) const override;

  /**
   * A std::set orders the columns ascending out of the box, which is desirable when printing them or comparing key
   * constraints.
   */
  std::set<ColumnID> _columns;

  KeyConstraintType _key_type;

  /**
   * Commit ID of the snapshot this constraint was last validated on. Note that the constraint will still be valid
   * during transactions with larger commit IDs if the table this constraint belongs to has not been modified since.
   * (Only insertions pose a problem, as deletions do not invalidate the constraint.)
   */
  mutable std::atomic<CommitID> _last_validated_on;
  /**
   * Commit ID of the snapshot this constraint was last invalidated on. Similarly, as above, this constraint will still
   * be invalid during transactions with larger commit IDs if only insertions have been performed since the last CID.
   */
  mutable std::atomic<CommitID> _last_invalidated_on;
};

using TableKeyConstraints = tbb::concurrent_unordered_set<TableKeyConstraint>;

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::TableKeyConstraint> {
  size_t operator()(const hyrise::TableKeyConstraint& table_key_constraint) const;
};

}  // namespace std
