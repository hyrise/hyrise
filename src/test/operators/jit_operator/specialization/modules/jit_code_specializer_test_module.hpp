#pragma once

#include <memory>
#include <vector>

namespace opossum {

/* A class hierarchy for testing the code specialization. The hierarchy roughly resembles the JitOperators, but
 * operates on simple integer values instead of database tuples. Each operation implements an apply function that
 * takes a value and applies some operation to it.
 * The Increment and Decrement operations are parameter-free, the IncrementByN operation takes a parameter via its
 * constructor that is stored in a private member variable. This operation is used to ensure information from member
 * variables are considered during code specialization.
 */
class AbstractOperation {
 public:
  virtual int32_t apply(const int32_t value) const = 0;

  virtual ~AbstractOperation() {}
};

class Increment : public AbstractOperation {
 public:
  int32_t apply(const int32_t value) const final { return value + 1; }
};

class Decrement : public AbstractOperation {
 public:
  int32_t apply(const int32_t value) const final { return value - 1; }
};

class IncrementByN : public AbstractOperation {
 public:
  explicit IncrementByN(const int32_t n) : _n{n} {}

  int32_t apply(const int32_t value) const final { return value + _n; }

 private:
  int32_t _n;
};

class MultipleOperations : public AbstractOperation {
 public:
  explicit MultipleOperations(const std::vector<std::shared_ptr<const AbstractOperation>> operations)
      : _operations{operations} {}

  int32_t apply(const int32_t value) const final {
    auto current_value = value;
    for (auto i = 0u; i < _operations.size(); ++i) {
      current_value = _operations[i]->apply(current_value);
    }
    return current_value;
  }

 private:
  std::vector<std::shared_ptr<const AbstractOperation>> _operations;
};

}  // namespace opossum
