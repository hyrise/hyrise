#pragma once

#include <vector>

namespace opossum {

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

}  // namespace opossum
