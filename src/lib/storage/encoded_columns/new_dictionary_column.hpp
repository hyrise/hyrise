#pragma once

#include <memory>
#include <string>

#include "base_encoded_column.hpp"
#include "types.hpp"

namespace opossum {

class BaseNsVector;

template <typename T>
class NewDictionaryColumn : public BaseEncodedColumn {
 public:
  explicit NewDictionaryColumn(const std::shared_ptr<const pmr_vector<T>>& dictionary,
                               const std::shared_ptr<const BaseNsVector>& attribute_vector,
                               const ValueID null_value_id);

  // returns an underlying dictionary
  std::shared_ptr<const pmr_vector<T>> dictionary() const;

  // returns an underlying data structure
  std::shared_ptr<const BaseNsVector> attribute_vector() const;

  // returns encoding specific null value ID
  ValueID null_value_id() const;

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  // return the number of entries
  size_t size() const final;

  // writes the length and value at the chunk_offset to the end of row_string
  void write_string_representation(std::string& row_string, const ChunkOffset chunk_offset) const final;

  // copies one of its own values to a different ValueColumn - mainly used for materialization
  // we cannot always use the materialize method below because sort results might come from different BaseColumns
  void copy_value_to_value_column(BaseColumn& value_column, ChunkOffset chunk_offset) const final;

  // Copies a column using a new allocator. This is useful for placing the column on a new NUMA node.
  std::shared_ptr<BaseColumn> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

  EncodingType encoding_type() const final;

 protected:
  const std::shared_ptr<const pmr_vector<T>> _dictionary;
  const std::shared_ptr<const BaseNsVector> _attribute_vector;
  const ValueID _null_value_id;
};

struct NewDictionaryColumnInfo {
  template <typename T>
  using ColumnTemplate = NewDictionaryColumn<T>;
};

}  // namespace opossum
