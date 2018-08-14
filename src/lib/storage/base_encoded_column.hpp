#pragma once

#include "storage/base_column.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

enum class CompressedVectorType : uint8_t;

/**
 * @brief Base class of all encoded columns
 *
 * Since encoded columns are immutable, all member variables
 * of sub-classes should be declared const.
 */
class BaseEncodedColumn : public BaseColumn {
 public:
  using BaseColumn::BaseColumn;

  // Encoded columns are immutable
  void append(const AllTypeVariant&) final;

  virtual EncodingType encoding_type() const = 0;

  /**
   * An encoded column may use a compressed vector to reduce its memory footprint.
   * Returns the vector’s type if it does, else CompressedVectorType::Invalid
   */
  virtual CompressedVectorType compressed_vector_type() const;
};

}  // namespace opossum
