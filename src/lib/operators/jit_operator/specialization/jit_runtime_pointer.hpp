#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <llvm/IR/Value.h>

#include "utils/assert.hpp"

namespace opossum {

/* The JitRuntimePointer class hierarchy represents concrete memory locations of LLVM bitcode pointer values.
 * A primer on LLVM pointer values:
 * LLVM provides a number of instructions for performing operations on pointer values.
 * LLVM, however, does not have any concept of concrete, materialized pointer values. Pointers represent the memory
 * location of a value in an abstract way, but LLVM has no way of knowing where a value will be located in memory at
 * compile-time. Nonetheless, several operations can be performed on these abstract pointer values.
 * - Bitcast instructions change the data type of a pointer, but does not change the value of a pointer.
 * - Load instructions load a piece of memory into a variable, similar to dereferencing pointers in C++.
 * - GetElementPtr instructions apply offsets to Thegetelementptrinstructiontakesapointertoacomplex value as its first argument. It then computes the offset to some member of the com- plex value, and applies it to the original pointer. The member is selected based on further integer parameters passed to the function. Since complex types in LLVM can be nested to arbitrary levels of depth, the instruction takes a variable number of integer arguments, one for each level of nesting. It is a common misconception that getelementptr instructions access memory themselves. They only perform pointer arithmetic to compute memory locations. Similar operations in C++ are the member of object access operator on structured types. The subscript operator on arrays, however, performs a dereferenciation and can thus not be considered equivalent.
 * These instructions can be combined to navigate arbitrarily nested and linked data structures.
 * We represent a set of LLVM pointer instructions that access some nested value by a chain of JitRuntimePointer
 * subclasses.
 * Each type of LLVM instruction is represented by a specific JitRuntimePointer subclass that can simulate the
 * equivalent operation on a "real" pointer.
 * All of these chains end with a JitConstantRuntimePointer that contains a constant pointer value. This value is not
 * fetched from the LLVM bitcode, but must be supplied by the caller. It serves as the source for runtime information.
 * Starting from this value, that chain of JitRuntimePointer instances can simulate the pointer operations in the LLVM
 * bitcode.
 * The JitRuntimePointer class hierarchy provides several helper functions to navigate these nested memory operations
 * (e.g., to get the total offset of consecutive GetElementPtr instructions).
 * The JitRuntimePointer base class represents pointer values that cannot be resolved to a physical address.
 */
class JitRuntimePointer {
 public:
  using Ptr = std::shared_ptr<const JitRuntimePointer>;

  virtual bool is_valid() const { return false; }
};

class JitKnownRuntimePointer : public JitRuntimePointer {
 public:
  using Ptr = std::shared_ptr<const JitKnownRuntimePointer>;

  // Checks whether the address pointed to is valid (i.e., can be dereferenced).
  // This solution is based on https://stackoverflow.com/questions/4611776/isbadreadptr-analogue-on-unix
  bool is_valid() const override {
    const auto ptr = reinterpret_cast<void*>(address());
    auto fd = open("/dev/random", O_WRONLY);
    bool result = (write(fd, ptr, 8) == 8);
    close(fd);
    return result;
  }

  // Returns the runtime address of this pointer
  virtual uint64_t address() const = 0;

  // Returns the total offset applied to a pointer across multiple GetElementPtr instructions
  virtual uint64_t total_offset() const = 0;

  // Returns the "base" pointer
  virtual const JitKnownRuntimePointer& base() const { Fail("Cannot get base pointer."); }
  virtual const JitKnownRuntimePointer& up() const { Fail("Cannot move pointer up."); }
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
  JitOffsetRuntimePointer(const std::shared_ptr<const JitKnownRuntimePointer>& base, const uint64_t offset)
      : _base{base}, _offset{offset} {}

  bool is_valid() const final { return _base->is_valid() && JitKnownRuntimePointer::is_valid(); }
  uint64_t address() const final { return _base->address() + _offset; }
  uint64_t total_offset() const final { return _base->total_offset() + _offset; }
  const JitKnownRuntimePointer& base() const final { return _base->base(); }
  const JitKnownRuntimePointer& up() const final { return _base->up(); }

 private:
  const std::shared_ptr<const JitKnownRuntimePointer> _base;
  const uint64_t _offset;
};

class JitDereferencedRuntimePointer : public JitKnownRuntimePointer {
 public:
  explicit JitDereferencedRuntimePointer(const std::shared_ptr<const JitKnownRuntimePointer>& base) : _base{base} {}

  bool is_valid() const final { return _base->is_valid() && JitKnownRuntimePointer::is_valid(); }
  uint64_t address() const final { return *reinterpret_cast<uint64_t*>(_base->address()); }
  uint64_t total_offset() const final { return 0L; }
  const JitKnownRuntimePointer& base() const final { return *this; }
  const JitKnownRuntimePointer& up() const final { return *_base; }

 private:
  const std::shared_ptr<const JitKnownRuntimePointer> _base;
};

}  // namespace opossum
