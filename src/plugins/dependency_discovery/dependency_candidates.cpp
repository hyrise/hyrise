#include "dependency_candidates.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>
#include "magic_enum.hpp"

#include "hyrise.hpp"

namespace hyrise {

// AbstractDependencyCandidate
AbstractDependencyCandidate::AbstractDependencyCandidate(const std::string& init_table_name,
                                                         const ColumnID init_column_id, const DependencyType init_type)
    : table_name{init_table_name}, column_id{init_column_id}, type{init_type} {}

bool AbstractDependencyCandidate::operator==(const AbstractDependencyCandidate& rhs) const {
  if (this == &rhs) {
    return true;
  }

  if (typeid(*this) != typeid(rhs) || table_name != rhs.table_name || column_id != rhs.column_id) {
    return false;
  }

  return _on_equals(rhs);
}

bool AbstractDependencyCandidate::operator!=(const AbstractDependencyCandidate& rhs) const {
  return !(rhs == *this);
}

size_t AbstractDependencyCandidate::hash() const {
  auto hash_value = boost::hash_value(type);
  boost::hash_combine(hash_value, table_name);
  boost::hash_combine(hash_value, column_id);
  boost::hash_combine(hash_value, _on_hash());
  return hash_value;
}

std::ostream& operator<<(std::ostream& stream, const AbstractDependencyCandidate& dependency_candidate) {
  return stream << dependency_candidate.description();
}

// UccCandidate
UccCandidate::UccCandidate(const std::string& init_table_name, const ColumnID init_column_id)
    : AbstractDependencyCandidate{init_table_name, init_column_id, DependencyType::UniqueColumn} {}

std::string UccCandidate::description() const {
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  auto stream = std::stringstream{};
  stream << "UCC " << table_name << "." << table->column_name(column_id);
  return stream.str();
}

size_t UccCandidate::_on_hash() const {
  return size_t{};
}

bool UccCandidate::_on_equals(const AbstractDependencyCandidate& rhs) const {
  return true;
}

// OdCandidate
OdCandidate::OdCandidate(const std::string& init_table_name, const ColumnID init_column_id,
                         const ColumnID init_ordered_column_id)
    : AbstractDependencyCandidate{init_table_name, init_column_id, DependencyType::Order},
      ordered_column_id{init_ordered_column_id} {}

std::string OdCandidate::description() const {
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  auto stream = std::stringstream{};
  stream << "OD " << table_name << "." << table->column_name(column_id) << " |-> " << table_name << "."
         << table->column_name(ordered_column_id);
  return stream.str();
}

size_t OdCandidate::_on_hash() const {
  return size_t{ordered_column_id};
}

bool OdCandidate::_on_equals(const AbstractDependencyCandidate& rhs) const {
  DebugAssert(dynamic_cast<const OdCandidate*>(&rhs),
              "Different dependency type should have been caught by AbstractDependencyCandidate::operator==");
  return ordered_column_id == static_cast<const OdCandidate&>(rhs).ordered_column_id;
}

// IndCandidate
IndCandidate::IndCandidate(const std::string& init_table_name, const ColumnID init_column_id,
                           const std::string& init_foreign_key_table, const ColumnID init_foreign_key_column_id)
    : AbstractDependencyCandidate{init_table_name, init_column_id, DependencyType::Inclusion},
      foreign_key_table{init_foreign_key_table},
      foreign_key_column_id{init_foreign_key_column_id} {}

std::string IndCandidate::description() const {
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  const auto& other_table = Hyrise::get().storage_manager.get_table(foreign_key_table);
  auto stream = std::stringstream{};
  stream << "IND " << foreign_key_table << "." << other_table->column_name(foreign_key_column_id) << " in "
         << table_name << "." << table->column_name(column_id);
  return stream.str();
}

size_t IndCandidate::_on_hash() const {
  auto hash_value = boost::hash_value(foreign_key_table);
  boost::hash_combine(hash_value, foreign_key_column_id);
  return hash_value;
}

bool IndCandidate::_on_equals(const AbstractDependencyCandidate& rhs) const {
  DebugAssert(dynamic_cast<const IndCandidate*>(&rhs),
              "Different dependency type should have been caught by AbstractDependencyCandidate::operator==");
  const auto& ind_candidate = static_cast<const IndCandidate&>(rhs);
  return foreign_key_table == ind_candidate.foreign_key_table &&
         foreign_key_column_id == ind_candidate.foreign_key_column_id;
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::AbstractDependencyCandidate>::operator()(
    const hyrise::AbstractDependencyCandidate& dependency_candidate) const {
  return dependency_candidate.hash();
}

}  // namespace std
