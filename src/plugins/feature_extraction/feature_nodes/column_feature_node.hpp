#pragma once

#include "abstract_feature_node.hpp"
#include "storage/table.hpp"

namespace opossum {

class ColumnFeatureNode : public AbstractFeatureNode {
 public:

  ColumnFeatureNode(const std::shared_ptr<AbstractFeatureNode>& input_node, const ColumnID column_id);

  size_t hash() const final;

  const std::vector<std::string>& feature_headers() const final;

  static const std::vector<std::string>& headers();

 protected:
  std::shared_ptr<FeatureVector> _on_to_feature_vector() final;

  ColumnID _column_id;
  std::shared_ptr<Table> _table;
  DataType _data_type;
  uint64_t _value_segments = 0;
  uint64_t _dictionary_segments = 0;
  uint64_t _fixed_string_dictionary_segments = 0;
  uint64_t _for_segments = 0;
  uint64_t _run_length_segments = 0;
  uint64_t _lz4_segments = 0;
  bool _nullable = false;
  uint64_t _sorted_segments = 0;

};

}  // namespace opossum
