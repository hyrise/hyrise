#pragma once

#include <unordered_set>

#include "types.hpp"

namespace hyrise {
/**
 * AbstractDependencyCandidate instances represent candidates for different data dependencies by one or more columns
 * (referencing the table by name and the column by ID). They are used to first collect all candidates for dependency
 * validation before actually validating them in the DependencyDiscoveryPlugin.
 */
enum class DependencyType { Order, Inclusion, UniqueColumn, Functional };

enum class ValidationStatus { Uncertain, Valid, Invalid, AlreadyKnown, Superfluous };

class AbstractDependencyCandidate : public Noncopyable {
 public:
  AbstractDependencyCandidate(const DependencyType init_type);

  AbstractDependencyCandidate() = delete;
  virtual ~AbstractDependencyCandidate() = default;

  bool operator==(const AbstractDependencyCandidate& rhs) const;
  bool operator!=(const AbstractDependencyCandidate& rhs) const;
  size_t hash() const;

  virtual std::string description() const = 0;

  const DependencyType type;

  mutable ValidationStatus status{ValidationStatus::Uncertain};

 protected:
  virtual size_t _on_hash() const = 0;
  virtual bool _on_equals(const AbstractDependencyCandidate& rhs) const = 0;
};

std::ostream& operator<<(std::ostream& stream, const AbstractDependencyCandidate& dependency_candidate);

// Wrapper around dependency_candidate->hash(), to enable hash based containers containing
// std::shared_ptr<AbstractDependencyCandidate>. Since we want to hold all candidates in a single data structure, we
// have to use pointers for the polymorphism to work.
struct DependencyCandidateSharedPtrHash final {
  size_t operator()(const std::shared_ptr<AbstractDependencyCandidate>& dependency_candidate) const {
    return dependency_candidate->hash();
  }

  size_t operator()(const std::shared_ptr<const AbstractDependencyCandidate>& dependency_candidate) const {
    return dependency_candidate->hash();
  }
};

// Wrapper around AbstractDependencyCandidate::operator==(), to enable hash based containers containing
// std::shared_ptr<AbstractDependencyCandidate>
struct DependencyCandidateSharedPtrEqual final {
  size_t operator()(const std::shared_ptr<const AbstractDependencyCandidate>& dependency_candidate_a,
                    const std::shared_ptr<const AbstractDependencyCandidate>& dependency_candidate_b) const {
    return dependency_candidate_a == dependency_candidate_b || *dependency_candidate_a == *dependency_candidate_b;
  }

  size_t operator()(const std::shared_ptr<AbstractDependencyCandidate>& dependency_candidate_a,
                    const std::shared_ptr<AbstractDependencyCandidate>& dependency_candidate_b) const {
    return dependency_candidate_a == dependency_candidate_b || *dependency_candidate_a == *dependency_candidate_b;
  }
};

// Note that operator== ignores the equality functions:
// https://stackoverflow.com/questions/36167764/can-not-compare-stdunorded-set-with-custom-keyequal
// If we want to prioritize dependency candidates in the future, this might be replaced or extended by a priority queue.
using DependencyCandidates = std::unordered_set<std::shared_ptr<AbstractDependencyCandidate>,
                                                DependencyCandidateSharedPtrHash, DependencyCandidateSharedPtrEqual>;

class UccCandidate : public AbstractDependencyCandidate {
 public:
  UccCandidate(const std::string& init_table_name, const ColumnID init_column_id);

  std::string description() const final;

  const std::string table_name;
  const ColumnID column_id;

 protected:
  size_t _on_hash() const final;
  bool _on_equals(const AbstractDependencyCandidate& rhs) const final;
};

class OdCandidate : public AbstractDependencyCandidate {
 public:
  OdCandidate(const std::string& init_table_name, const ColumnID init_ordering_column_id,
              const ColumnID init_ordered_column_id);

  std::string description() const final;

  const std::string table_name;
  const ColumnID ordering_column_id;
  const ColumnID ordered_column_id;

 protected:
  size_t _on_hash() const final;
  bool _on_equals(const AbstractDependencyCandidate& rhs) const final;
};

class IndCandidate : public AbstractDependencyCandidate {
 public:
  IndCandidate(const std::string& init_foreign_key_table, const ColumnID init_foreign_key_column_id,
               const std::string& init_primary_key_table, const ColumnID init_primary_key_column_id);

  std::string description() const final;

  const std::string foreign_key_table;
  const ColumnID foreign_key_column_id;

  const std::string primary_key_table;
  const ColumnID primary_key_column_id;

  mutable DependencyCandidates dependents;

 protected:
  size_t _on_hash() const final;
  bool _on_equals(const AbstractDependencyCandidate& rhs) const final;
};

class FdCandidate : public AbstractDependencyCandidate {
 public:
  FdCandidate(const std::string& init_table_name, const std::unordered_set<ColumnID>& init_column_ids);

  std::string description() const final;

  const std::string table_name;
  // Ordered to prefer integral over floating-point over string columns (columns with same types are ordered by ID).
  const std::vector<ColumnID> column_ids;

 protected:
  size_t _on_hash() const final;
  bool _on_equals(const AbstractDependencyCandidate& rhs) const final;
};

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::AbstractDependencyCandidate> {
  size_t operator()(const hyrise::AbstractDependencyCandidate& dependency_candidate) const;
};

}  // namespace std
