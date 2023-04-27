#include "dependency_candidates.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>
#include "magic_enum.hpp"

#include "hyrise.hpp"
#include "resolve_type.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

// Need this additional method to sort the ColumnIDs in the initializer list. We cannot initialize the const member otherwise.
std::vector<ColumnID> sort_fd_candidate_columns(const std::string& table_name,
                                                const std::unordered_set<ColumnID>& column_ids) {
  // Prioritize columns with integral data type. They can use more validation shortcuts and grouping with int32_t is
  // also faster in AggregateHash.
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  auto columns = std::vector<ColumnID>{column_ids.cbegin(), column_ids.cend()};

  // Return true if lhs is integral and rhs is not. Else, return lhs < rhs.
  const auto compare_column_ids_by_type = [&](const auto lhs, const auto rhs) {
    auto is_smaller = false;
    resolve_data_type(table->column_data_type(lhs), [&](auto lhs_type) {
      using LhsDataType = typename decltype(lhs_type)::type;
      resolve_data_type(table->column_data_type(rhs), [&](auto rhs_type) {
        using RhsDataType = typename decltype(rhs_type)::type;

        if constexpr (std::is_integral_v<LhsDataType> && !std::is_integral_v<RhsDataType>) {
          is_smaller = true;
          return;
        }

        if constexpr (!std::is_integral_v<LhsDataType> && std::is_integral_v<RhsDataType>) {
          return;
        }

        if constexpr (std::is_arithmetic_v<LhsDataType> && !std::is_arithmetic_v<RhsDataType>) {
          is_smaller = true;
          return;
        }

        if constexpr (!std::is_arithmetic_v<LhsDataType> && std::is_arithmetic_v<RhsDataType>) {
          return;
        }

        // Both have the same type, just compare the ColumnIDs then.
        is_smaller = lhs < rhs;
      });
    });

    return is_smaller;
  };

  std::sort(columns.begin(), columns.end(), compare_column_ids_by_type);
  return columns;
}

}  // namespace

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
      column_ids{sort_fd_candidate_columns(init_table_name, init_column_ids)} {
  Assert(column_ids.size() > 1, "Expected multiple columns.");
}

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

  const auto column_id_count = column_ids.size();
  if (column_id_count != fd_candidate.column_ids.size()) {
    return false;
  }

  for (auto column_id = ColumnID{0}; column_id < column_id_count; ++column_id) {
    if (column_ids[column_id] != fd_candidate.column_ids[column_id]) {
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
