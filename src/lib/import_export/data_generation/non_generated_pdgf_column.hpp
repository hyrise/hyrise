#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "all_type_variant.hpp"
#include "storage/abstract_segment.hpp"
#include "types.hpp"

namespace hyrise {
class BaseNonGeneratedPDGFColumn {
 public:
  explicit BaseNonGeneratedPDGFColumn();
  virtual ~BaseNonGeneratedPDGFColumn() = default;
  virtual std::string name() = 0;
  virtual DataType type() = 0;
  virtual std::shared_ptr<AbstractSegment> build_segment(ChunkOffset chunk_size) = 0;
};

template <typename T>
class NonGeneratedPDGFColumn : public BaseNonGeneratedPDGFColumn {
 public:
  explicit NonGeneratedPDGFColumn(std::string name, DataType type);
  std::string name() override;
  DataType type() override;
  std::shared_ptr<AbstractSegment> build_segment(ChunkOffset chunk_size) override;

 protected:
  std::string _name;
  DataType _type;
};
} // namespace hyrise
