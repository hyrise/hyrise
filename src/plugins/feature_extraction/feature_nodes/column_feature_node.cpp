#include "column_feature_node.hpp"

#include "feature_extraction/util/one_hot_encoding.hpp"
#include "feature_extraction/feature_nodes/table_feature_node.hpp"
#include "hyrise.hpp"
#include "storage/abstract_encoded_segment.hpp"

namespace opossum {

  ColumnFeatureNode::ColumnFeatureNode(const std::shared_ptr<AbstractFeatureNode>& input_node, const ColumnID column_id): AbstractFeatureNode{FeatureNodeType::Column, input_node}, _column_id{column_id} {
    Assert(_left_input->type() == FeatureNodeType::Table, "ColumnFeatureNode requires TableFeatureNode as input");
    const auto& table_feature_node = static_cast<TableFeatureNode&>(*_left_input);
    Assert(table_feature_node.table_name(), "ColumnFeatureNode requires original TableFeatureNode as input");
    const auto& table = Hyrise::get().storage_manager.get_table(*table_feature_node.table_name());
    _table = table;
    _data_type = table->column_data_type(_column_id);
  }

  
  size_t ColumnFeatureNode::hash() const {
    auto hash = _left_input->hash();

    // hashes should already be unique, and operators have only one output
    // thus, let's just add the table type to differ from the input hashes
    boost::hash_combine(hash, _column_id);
    return hash;
  }

  std::shared_ptr<FeatureVector> ColumnFeatureNode::_on_to_feature_vector() {
    const auto chunk_count = _table->chunk_count();
    _nullable = _table->column_is_nullable(_column_id);
    _data_type = _table->column_data_type(_column_id);
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto& chunk = _table->get_chunk(chunk_id);
      if (!chunk) {
        continue;
      }

      const auto& segments_sorted_by = chunk->individually_sorted_by();
      if (std::any_of(segments_sorted_by.cbegin(), segments_sorted_by.cend(), [&](const auto& sort_definition){ return sort_definition.column == _column_id; })) {
        ++_sorted_segments;
      }

      const auto& segment = chunk->get_segment(_column_id);
      if (auto encoded_segment = std::dynamic_pointer_cast<AbstractEncodedSegment>(segment)) {
        switch (encoded_segment->encoding_type()) {
          case EncodingType::Unencoded:
            Fail("Did not expect unencoded Segment");
          case EncodingType::Dictionary:
            ++_dictionary_segments;
            break;
          case EncodingType::FrameOfReference:
            ++_for_segments;
            break;
          case EncodingType::FixedStringDictionary:
            ++_fixed_string_dictionary_segments;
            break;
          case EncodingType::LZ4:
            ++_lz4_segments;
            break;
          case EncodingType::RunLength:
            ++_run_length_segments;
            break;
        }
      } else {
        ++_value_segments;
      }

    }
    auto feature_vector = one_hot_encoding<DataType>(_data_type);
    feature_vector->reserve(_feature_vector->size() + 8);
    feature_vector->emplace_back(static_cast<Feature>(_nullable));
    const auto num_chunks = static_cast<double>(chunk_count);
    feature_vector->emplace_back(static_cast<Feature>(_value_segments) / num_chunks);
    feature_vector->emplace_back(static_cast<Feature>(_dictionary_segments) / num_chunks);
    feature_vector->emplace_back(static_cast<Feature>(_run_length_segments) / num_chunks);
    feature_vector->emplace_back(static_cast<Feature>(_fixed_string_dictionary_segments) / num_chunks);
    feature_vector->emplace_back(static_cast<Feature>(_for_segments) / num_chunks);
    feature_vector->emplace_back(static_cast<Feature>(_lz4_segments) / num_chunks);
    feature_vector->emplace_back(static_cast<Feature>(_sorted_segments) / num_chunks);

    return feature_vector;
  }

  const std::vector<std::string>& ColumnFeatureNode::feature_headers() const {
    return headers();
  }

  const std::vector<std::string>& ColumnFeatureNode::headers() {
    static auto ohe_headers_type = one_hot_headers<DataType>("data_type.");
    static const auto headers = std::vector<std::string>{"nullable", "value_segments", "dictionary_segments", "run_length_segments", "fixed_string_segments", "for_segments", "lz4_segments", "sorted_segments"};
    if (ohe_headers_type.size() == magic_enum::enum_count<DataType>()) {
      ohe_headers_type.insert(ohe_headers_type.end(), headers.begin(), headers.end());
    }
    return ohe_headers_type;

  }

}  // namespace opossum
