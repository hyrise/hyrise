#include "dependency_candidates.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>
#include "magic_enum.hpp"

#include "hyrise.hpp"

namespace hyrise {

// AbstractDependencyCandidate
AbstractDependencyCandidate::AbstractDependencyCandidate(const DependencyType init_type) : type{init_type} {}

bool AbstractDependencyCandidate::operator==(const AbstractDependencyCandidate& rhs) const {
  if (this == &rhs) {
    return true;
  }

  if (typeid(*this) != typeid(rhs)) {
    return false;
  }

  return _on_equals(rhs);
}

bool AbstractDependencyCandidate::operator!=(const AbstractDependencyCandidate& rhs) const {
  return !(rhs == *this);
}

size_t AbstractDependencyCandidate::hash() const {
  auto hash_value = boost::hash_value(type);
  boost::hash_combine(hash_value, _on_hash());
  return hash_value;
}

std::ostream& operator<<(std::ostream& stream, const AbstractDependencyCandidate& dependency_candidate) {
  return stream << dependency_candidate.description();
}

// UccCandidate
UccCandidate::UccCandidate(const std::string& init_table_name, const ColumnID init_column_id)
    : AbstractDependencyCandidate{DependencyType::UniqueColumn},
      table_name(init_table_name),
      column_id{init_column_id} {}

std::string UccCandidate::description() const {
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  auto stream = std::stringstream{};
  stream << "UCC " << table_name << "." << table->column_name(column_id);
  return stream.str();
}

size_t UccCandidate::_on_hash() const {
  auto hash_value = boost::hash_value(table_name);
  boost::hash_combine(hash_value, column_id);
  return hash_value;
}

bool UccCandidate::_on_equals(const AbstractDependencyCandidate& rhs) const {
  DebugAssert(dynamic_cast<const UccCandidate*>(&rhs),
              "Different dependency type should have been caught by AbstractDependencyCandidate::operator==");
  const auto& ucc_candidate = static_cast<const UccCandidate&>(rhs);
  return table_name == ucc_candidate.table_name && column_id == ucc_candidate.column_id;
}

// OdCandidate
OdCandidate::OdCandidate(const std::string& init_table_name, const ColumnID init_ordering_column_id,
                         const ColumnID init_ordered_column_id)
    : AbstractDependencyCandidate{DependencyType::Order},
      table_name{init_table_name},
      ordering_column_id{init_ordering_column_id},
      ordered_column_id{init_ordered_column_id} {}

std::string OdCandidate::description() const {
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  auto stream = std::stringstream{};
  stream << "OD " << table_name << "." << table->column_name(ordering_column_id) << " |-> " << table_name << "."
         << table->column_name(ordered_column_id);
  return stream.str();
}

size_t OdCandidate::_on_hash() const {
  auto hash_value = boost::hash_value(table_name);
  boost::hash_combine(hash_value, ordering_column_id);
  boost::hash_combine(hash_value, ordered_column_id);
  return hash_value;
}

bool OdCandidate::_on_equals(const AbstractDependencyCandidate& rhs) const {
  DebugAssert(dynamic_cast<const OdCandidate*>(&rhs),
              "Different dependency type should have been caught by AbstractDependencyCandidate::operator==");
  const auto& od_candidate = static_cast<const OdCandidate&>(rhs);
  return table_name == od_candidate.table_name && ordering_column_id == od_candidate.ordering_column_id &&
         ordered_column_id == od_candidate.ordered_column_id;
}

// IndCandidate
IndCandidate::IndCandidate(const std::string& init_foreign_key_table, const ColumnID init_foreign_key_column_id,
                           const std::string& init_primary_key_table, const ColumnID init_primary_key_column_id)
    : AbstractDependencyCandidate{DependencyType::Inclusion},
      foreign_key_table{init_foreign_key_table},
      foreign_key_column_id{init_foreign_key_column_id},
      primary_key_table{init_primary_key_table},
      primary_key_column_id{init_primary_key_column_id} {}

std::string IndCandidate::description() const {
  const auto& foreign_key_table_ref = Hyrise::get().storage_manager.get_table(foreign_key_table);
  const auto& primary_key_table_ref = Hyrise::get().storage_manager.get_table(primary_key_table);
  auto stream = std::stringstream{};
  stream << "IND " << foreign_key_table << "." << foreign_key_table_ref->column_name(foreign_key_column_id) << " in "
         << primary_key_table << "." << primary_key_table_ref->column_name(primary_key_column_id);
  return stream.str();
}

size_t IndCandidate::_on_hash() const {
  auto hash_value = boost::hash_value(foreign_key_table);
  boost::hash_combine(hash_value, foreign_key_column_id);
  boost::hash_combine(hash_value, primary_key_table);
  boost::hash_combine(hash_value, primary_key_column_id);
  return hash_value;
}

bool IndCandidate::_on_equals(const AbstractDependencyCandidate& rhs) const {
  DebugAssert(dynamic_cast<const IndCandidate*>(&rhs),
              "Different dependency type should have been caught by AbstractDependencyCandidate::operator==");
  const auto& ind_candidate = static_cast<const IndCandidate&>(rhs);
  return foreign_key_table == ind_candidate.foreign_key_table &&
         foreign_key_column_id == ind_candidate.foreign_key_column_id &&
         primary_key_table == ind_candidate.primary_key_table &&
         primary_key_column_id == ind_candidate.primary_key_column_id;
}

// FdCandidate
FdCandidate::FdCandidate(const std::string& init_table_name, const std::unordered_set<ColumnID>& init_column_ids)
    : AbstractDependencyCandidate{DependencyType::Functional},
      table_name{init_table_name},
      column_ids{init_column_ids} {}

std::string FdCandidate::description() const {
  auto stream = std::stringstream{};
  stream << "FD ";
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);

  for (auto column_id_it = column_ids.cbegin(); column_id_it != column_ids.cend(); ++column_id_it) {
    stream << table_name << "." << table->column_name(*column_id_it);
    if (std::next(column_id_it) != column_ids.cend()) {
      stream << ", ";
    }
  }
  return stream.str();
}

size_t FdCandidate::_on_hash() const {
  auto hash_value = boost::hash_value(table_name);
  boost::hash_combine(hash_value, column_ids.size());
  for (const auto column_id : column_ids) {
    hash_value = hash_value ^ column_id;
  }
  return hash_value;
}

bool FdCandidate::_on_equals(const AbstractDependencyCandidate& rhs) const {
  DebugAssert(dynamic_cast<const FdCandidate*>(&rhs),
              "Different dependency type should have been caught by AbstractDependencyCandidate::operator==");
  const auto& fd_candidate = static_cast<const FdCandidate&>(rhs);
  if (table_name != fd_candidate.table_name) {
    return false;
  }

  if (column_ids.size() != fd_candidate.column_ids.size()) {
    return false;
  }

  for (const auto column_id : column_ids) {
    if (!fd_candidate.column_ids.contains(column_id)) {
      return false;
    }
  }

  return true;
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::AbstractDependencyCandidate>::operator()(
    const hyrise::AbstractDependencyCandidate& dependency_candidate) const {
  return dependency_candidate.hash();
}

}  // namespace std
