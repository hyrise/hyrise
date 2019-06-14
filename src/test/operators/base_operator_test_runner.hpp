#pragma once

#include <fstream>
#include <string>

#include "base_test.hpp"
#include "storage/pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

namespace opossum {

enum class InputSide { Left, Right };

// Operators might build internal PosLists that they have to de-reference when assembling the output Table, if the
// input itself is already a reference Table.
enum class InputTableType {
  // Input Tables are data
  Data,
  // Input Tables are reference Tables with all Segments of a Chunk having the same PosList.
  SharedPosList,
  // Input Tables are reference Tables with each Segment using a different PosList
  IndividualPosLists
};

extern std::unordered_map<InputTableType, std::string> input_table_type_to_string;

struct InputTableConfiguration {
  InputSide side{};
  ChunkOffset chunk_size{};
  size_t table_size{};
  InputTableType table_type{};
  EncodingType encoding_type{EncodingType::Unencoded};

  auto to_tuple() const { return std::tie(side, chunk_size, table_size, table_type, encoding_type); }
};

bool operator<(const InputTableConfiguration& l, const InputTableConfiguration& r);
bool operator==(const InputTableConfiguration& l, const InputTableConfiguration& r);

template <typename TestConfiguration>
class BaseOperatorTestRunner : public BaseTestWithParam<TestConfiguration> {
 public:
  BaseOperatorTestRunner(const std::string& input_table_path) : input_table_path(input_table_path) {}

  std::string get_table_path(const InputTableConfiguration& key) {
    const auto& [side, chunk_size, table_size, input_table_type, encoding_type] = key;

    const auto side_str = side == InputSide::Left ? "left" : "right";
    const auto table_size_str = std::to_string(table_size);

    return input_table_path + "input_table_" + side_str + "_" + table_size_str + ".tbl";
  }

  std::shared_ptr<Table> get_table(const InputTableConfiguration& key) {
    auto input_table_iter = input_tables.find(key);
    if (input_table_iter == input_tables.end()) {
      const auto& [side, chunk_size, table_size, input_table_type, encoding_type] = key;
      std::ignore = side;
      std::ignore = table_size;

      auto table = load_table(get_table_path(key), chunk_size);

      /**
       * Encode the table, if requested. Encode only those columns whose DataTypes are supported by the requested
       * encoding.
       */
      if (key.encoding_type != EncodingType::Unencoded) {
        auto chunk_encoding_spec = ChunkEncodingSpec{table->column_count()};
        for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
          if (encoding_supports_data_type(key.encoding_type, table->column_data_type(column_id))) {
            chunk_encoding_spec[column_id] = {key.encoding_type};
          } else {
            chunk_encoding_spec[column_id] = {EncodingType::Unencoded};
          }
        }
        ChunkEncoder::encode_all_chunks(table, chunk_encoding_spec);
      }

      /**
       * Create a Reference-Table pointing 1-to-1 and in-order to the rows in the original table. This tests the
       * writing of the output table in the JoinOperator, which has to make sure not to create ReferenceSegments
       * pointing to ReferenceSegments.
       */
      if (input_table_type != InputTableType::Data) {
        const auto reference_table = std::make_shared<Table>(table->column_definitions(), TableType::References);

        for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
          const auto input_chunk = table->get_chunk(chunk_id);

          Segments reference_segments;

          if (input_table_type == InputTableType::SharedPosList) {
            const auto pos_list = std::make_shared<PosList>();
            for (auto chunk_offset = ChunkOffset{0}; chunk_offset < input_chunk->size(); ++chunk_offset) {
              pos_list->emplace_back(chunk_id, chunk_offset);
            }

            for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
              reference_segments.emplace_back(std::make_shared<ReferenceSegment>(table, column_id, pos_list));
            }

          } else if (input_table_type == InputTableType::IndividualPosLists) {
            for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
              const auto pos_list = std::make_shared<PosList>();
              for (auto chunk_offset = ChunkOffset{0}; chunk_offset < input_chunk->size(); ++chunk_offset) {
                pos_list->emplace_back(chunk_id, chunk_offset);
              }

              reference_segments.emplace_back(std::make_shared<ReferenceSegment>(table, column_id, pos_list));
            }
          }

          reference_table->append_chunk(reference_segments);
        }

        table = reference_table;
      }

      input_table_iter = input_tables.emplace(key, table).first;
    }

    return input_table_iter->second;
  }

  virtual ~BaseOperatorTestRunner() = default;

  std::string input_table_path;

  static inline std::map<InputTableConfiguration, std::shared_ptr<Table>> input_tables;

  // Cache verification table to avoid redundant computation of the same
  static inline std::map<TestConfiguration, std::shared_ptr<const Table>> expected_output_tables;

  static inline const auto all_input_table_types =
      std::vector{InputTableType::Data, InputTableType::IndividualPosLists, InputTableType::SharedPosList};

  static inline const auto all_encoding_types =
      std::vector{EncodingType::Unencoded,        EncodingType::Dictionary,
                  EncodingType::FrameOfReference, EncodingType::FixedStringDictionary,
                  EncodingType::RunLength,        EncodingType::LZ4};
};

}  // namespace opossum
