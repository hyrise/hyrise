#pragma once

#include <fcntl.h>

#include <llvm/IR/Value.h>

namespace opossum {

class JitRuntimePointer {
 public:
  using Ptr = std::shared_ptr<const JitRuntimePointer>;

  virtual bool is_valid() const { return false; }
};

class JitKnownRuntimePointer : public JitRuntimePointer {
 public:
  using Ptr = std::shared_ptr<const JitKnownRuntimePointer>;

  bool is_valid() const override {
    const auto ptr = reinterpret_cast<void*>(address());
    auto fd = open("/dev/random", O_WRONLY);
    bool result = (write(fd, ptr, 8) == 8);
    close(fd);
    return result;
  }

  virtual uint64_t address() const = 0;
  virtual uint64_t total_offset() const = 0;
  virtual const JitKnownRuntimePointer& base() const { throw std::logic_error("can't get base pointer"); }
  virtual const JitKnownRuntimePointer& up() const { throw std::logic_error("can't move up"); }
};

class JitConstantRuntimePointer : public JitKnownRuntimePointer {
 public:
  explicit JitConstantRuntimePointer(const uint64_t address) : _address{address} {}
  template <typename T>
  explicit JitConstantRuntimePointer(const T* ptr) : _address{reinterpret_cast<uint64_t>(ptr)} {}
  template <typename T>
  explicit JitConstantRuntimePointer(const std::shared_ptr<T>& ptr) : _address{reinterpret_cast<uint64_t>(ptr.get())} {}

  uint64_t address() const final { return _address; }
  uint64_t total_offset() const final { return 0L; }
  const JitKnownRuntimePointer& base() const final { return *this; }

 private:
  const uint64_t _address;
};

class JitOffsetRuntimePointer : public JitKnownRuntimePointer {
 public:
  JitOffsetRuntimePointer(const JitKnownRuntimePointer::Ptr& base, const uint64_t offset)
      : _base{base}, _offset{offset} {}

  bool is_valid() const final { return _base->is_valid() && JitKnownRuntimePointer::is_valid(); }
  uint64_t address() const final { return _base->address() + _offset; }
  uint64_t total_offset() const final { return _base->total_offset() + _offset; }
  const JitKnownRuntimePointer& base() const final { return _base->base(); }
  const JitKnownRuntimePointer& up() const final { return _base->up(); }

 private:
  const JitKnownRuntimePointer::Ptr _base;
  const uint64_t _offset;
};

class JitDereferencedRuntimePointer : public JitKnownRuntimePointer {
 public:
  explicit JitDereferencedRuntimePointer(const JitKnownRuntimePointer::Ptr& base) : _base{base} {}

  bool is_valid() const final { return _base->is_valid() && JitKnownRuntimePointer::is_valid(); }
  uint64_t address() const final { return *reinterpret_cast<uint64_t*>(_base->address()); }
  uint64_t total_offset() const final { return 0L; }
  const JitKnownRuntimePointer& base() const final { return *this; }
  const JitKnownRuntimePointer& up() const final { return *_base; }

 private:
  const JitKnownRuntimePointer::Ptr _base;
};

}  // namespace opossum
