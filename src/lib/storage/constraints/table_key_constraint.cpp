#include "table_key_constraint.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <functional>
#include <set>
#include <utility>

#include <boost/container_hash/hash.hpp>

#include "storage/constraints/abstract_table_constraint.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/atomic_max.hpp"

namespace hyrise {

TableKeyConstraint::TableKeyConstraint(std::set<ColumnID>&& columns, const KeyConstraintType key_type,
                                       const CommitID last_validated_on, const CommitID last_invalidated_on)
    : AbstractTableConstraint(TableConstraintType::Key),
      _columns{std::move(columns)},
      _key_type{key_type},
      _last_validated_on{last_validated_on},
      _last_invalidated_on{last_invalidated_on} {
  Assert(!_columns.empty(), "Did not expect useless constraint.");
  Assert(key_type != KeyConstraintType::PRIMARY_KEY ||
             (last_validated_on == MAX_COMMIT_ID && last_invalidated_on == MAX_COMMIT_ID),
         "A key constraint of type PRIMARY_KEY must be genuine.");
}

TableKeyConstraint::TableKeyConstraint(const TableKeyConstraint& other)
    : AbstractTableConstraint(other),
      _columns{other._columns},
      _key_type{other._key_type},
      _last_validated_on{other._last_validated_on.load()},
      _last_invalidated_on{other._last_invalidated_on.load()} {}

TableKeyConstraint& TableKeyConstraint::operator=(const TableKeyConstraint& other) {
  if (this != &other) {
    AbstractTableConstraint::operator=(other);
    _columns = other._columns;
    _key_type = other._key_type;
    _last_validated_on.store(other._last_validated_on.load());
    _last_invalidated_on.store(other._last_invalidated_on.load());
  }
  return *this;
}

TableKeyConstraint::TableKeyConstraint(TableKeyConstraint&& other) noexcept
    : AbstractTableConstraint(other.type()),
      _columns{std::move(other._columns)},
      _key_type{other._key_type},
      _last_validated_on{other._last_validated_on.load()},
      _last_invalidated_on{other._last_invalidated_on.load()} {}

TableKeyConstraint& TableKeyConstraint::operator=(TableKeyConstraint&& other) noexcept {
  if (this != &other) {
    _columns = std::move(other._columns);
    _key_type = other._key_type;
    _last_validated_on.store(other._last_validated_on.load());
    _last_invalidated_on.store(other._last_invalidated_on.load());
  }
  return *this;
}

const std::set<ColumnID>& TableKeyConstraint::columns() const {
  return _columns;
}

KeyConstraintType TableKeyConstraint::key_type() const {
  return _key_type;
}

bool TableKeyConstraint::can_become_invalid() const {
  return _last_validated_on.load() != MAX_COMMIT_ID;
}

ValidationResultType TableKeyConstraint::last_validation_result() const {
  const auto last_invalidated = _last_invalidated_on.load();
  const auto last_validated = _last_validated_on.load();

  if (!can_become_invalid() || last_invalidated == MAX_COMMIT_ID ||
      (last_validated != MAX_COMMIT_ID && last_validated > last_invalidated)) {
    return ValidationResultType::VALID;
  }
  return ValidationResultType::INVALID;
}

CommitID TableKeyConstraint::last_validated_on() const {
  return _last_validated_on.load();
}

CommitID TableKeyConstraint::last_invalidated_on() const {
  return _last_invalidated_on.load();
}

void TableKeyConstraint::revalidated_on(const CommitID revalidation_commit_id) const {
  // Do not revalidate a genuine constraint as this would make it spurious.
  Assert(can_become_invalid(), "Cannot invalidate UCC that cannot become invalid.");

  set_atomic_max(_last_validated_on, revalidation_commit_id);
}

void TableKeyConstraint::invalidated_on(const CommitID invalidation_commit_id) const {
  // Do not revalidate a genuine constraint as this would make it spurious.
  Assert(can_become_invalid(), "Cannot invalidate UCC that cannot become invalid.");

  set_atomic_max(_last_invalidated_on, invalidation_commit_id);
}

size_t TableKeyConstraint::hash() const {
  auto hash = size_t{0};
  boost::hash_combine(hash, _key_type);
  for (const auto& column : _columns) {
    boost::hash_combine(hash, column);
  }
  return hash;
}

bool TableKeyConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const TableKeyConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  const auto& rhs = static_cast<const TableKeyConstraint&>(table_constraint);
  return _key_type == rhs._key_type && _columns == rhs._columns;
}

bool TableKeyConstraint::operator<(const TableKeyConstraint& rhs) const {
  // PRIMARY_KEY constraints are "smaller" than UNIQUE constraints. Thus, they are listed first when printing them.
  if (_key_type != rhs._key_type) {
    return _key_type < rhs._key_type;
  }

  // As the columns are stored in a std::set, iteration is sorted and the result is not ambiguous.
  return std::ranges::lexicographical_compare(_columns, rhs._columns);
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::TableKeyConstraint>::operator()(const hyrise::TableKeyConstraint& table_key_constraint) const {
  return table_key_constraint.hash();
}

}  // namespace std
