#include <fstream>
#include <set>

#include "base_test.hpp"
#include "nlohmann/json.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_index.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/join_verification.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/index/b_tree/b_tree_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "utils/load_table.hpp"
#include "utils/make_bimap.hpp"

/**
 * This file contains the main tests for Hyrise's join operators.
 * Testing is done by comparing the result of any given join operator with that of the JoinVerification for a
 * number of configurations (i.e. JoinModes, predicates, data types, ...)
 */

using namespace std::string_literals;  // NOLINT

namespace {

using namespace opossum;  // NOLINT

using ChunkRange = std::pair<ChunkID, ChunkID>;

enum class InputSide { Left, Right };

// Join operators might build internal PosLists that they have to de-reference when assembling the output Table if the
// input itself is already a reference Table.
enum class InputTableType {
  // Input Tables are data
  Data,
  // Input Tables are reference Tables with all Segments of a Chunk having the same PosList.
  SharedPosList,
  // Input Tables are reference Tables with each Segment using a different PosList
  IndividualPosLists
};

std::unordered_map<InputTableType, std::string> input_table_type_to_string{
    {InputTableType::Data, "Data"},
    {InputTableType::SharedPosList, "SharedPosList"},
    {InputTableType::IndividualPosLists, "IndividualPosLists"}};

struct InputTableConfiguration {
  InputSide side{};
  ChunkOffset chunk_size{};
  size_t table_size{};
  InputTableType table_type{};
  EncodingType encoding_type{EncodingType::Unencoded};

  // Only for JoinIndex
  ChunkRange indexed_chunk_range{};           // chunk range of indexed join column segments
  ChunkRange single_chunk_reference_range{};  // chunk range of join column segments that reference only one chunk

  auto to_tuple() const {
    return std::tie(side, chunk_size, table_size, table_type, encoding_type, indexed_chunk_range,
                    single_chunk_reference_range);
  }
};

bool operator<(const InputTableConfiguration& l, const InputTableConfiguration& r) {
  return l.to_tuple() < r.to_tuple();
}
bool operator==(const InputTableConfiguration& l, const InputTableConfiguration& r) {
  return l.to_tuple() == r.to_tuple();
}

class BaseJoinOperatorFactory;

struct JoinTestConfiguration {
  InputTableConfiguration left_input;
  InputTableConfiguration right_input;
  JoinMode join_mode{JoinMode::Inner};
  DataType data_type_left{DataType::Int};
  DataType data_type_right{DataType::Int};
  bool nullable_left{false};
  bool nullable_right{false};
  PredicateCondition predicate_condition{PredicateCondition::Equals};
  std::vector<OperatorJoinPredicate> secondary_predicates;
  std::shared_ptr<BaseJoinOperatorFactory> join_operator_factory;

  // Only for JoinHash
  std::optional<size_t> radix_bits;

  // Only for JoinIndex
  std::optional<IndexSide> index_side;

  void swap_input_sides() {
    std::swap(left_input, right_input);
    std::swap(data_type_left, data_type_right);
    std::swap(nullable_left, nullable_right);

    for (auto& secondary_predicate : secondary_predicates) {
      std::swap(secondary_predicate.column_ids.first, secondary_predicate.column_ids.second);
      secondary_predicate.predicate_condition = flip_predicate_condition(secondary_predicate.predicate_condition);
    }
  }

  auto to_tuple() const {
    return std::tie(left_input, right_input, join_mode, data_type_left, data_type_right, nullable_left, nullable_right,
                    predicate_condition, secondary_predicates);
  }
};

bool operator<(const JoinTestConfiguration& l, const JoinTestConfiguration& r) { return l.to_tuple() < r.to_tuple(); }
bool operator==(const JoinTestConfiguration& l, const JoinTestConfiguration& r) { return l.to_tuple() == r.to_tuple(); }

// Virtual interface to create a join operator
class BaseJoinOperatorFactory {
 public:
  virtual ~BaseJoinOperatorFactory() = default;
  virtual std::shared_ptr<AbstractJoinOperator> create_operator(const std::shared_ptr<const AbstractOperator>& left,
                                                                const std::shared_ptr<const AbstractOperator>& right,
                                                                const OperatorJoinPredicate& primary_predicate,
                                                                const JoinTestConfiguration& configuration) = 0;
};

template <typename JoinOperator>
class JoinOperatorFactory : public BaseJoinOperatorFactory {
 public:
  std::shared_ptr<AbstractJoinOperator> create_operator(const std::shared_ptr<const AbstractOperator>& left,
                                                        const std::shared_ptr<const AbstractOperator>& right,
                                                        const OperatorJoinPredicate& primary_predicate,
                                                        const JoinTestConfiguration& configuration) override {
    if constexpr (std::is_same_v<JoinOperator, JoinHash>) {
      return std::make_shared<JoinOperator>(left, right, configuration.join_mode, primary_predicate,
                                            configuration.secondary_predicates, configuration.radix_bits);
    } else if constexpr (std::is_same_v<JoinOperator, JoinIndex>) {  // NOLINT
      Assert(configuration.index_side, "IndexSide should be explicitly defined for the JoinIndex test runs.");
      return std::make_shared<JoinIndex>(left, right, configuration.join_mode, primary_predicate,
                                         configuration.secondary_predicates, *configuration.index_side);
    } else {
      return std::make_shared<JoinOperator>(left, right, configuration.join_mode, primary_predicate,
                                            configuration.secondary_predicates);
    }
  }
};
// Order of columns in the input tables
const std::unordered_map<DataType, size_t> data_type_order = {
    {DataType::Int, 0u}, {DataType::Float, 1u}, {DataType::Double, 2u}, {DataType::Long, 3u}, {DataType::String, 4u},
};

}  // namespace

namespace opossum {

class JoinTestRunner : public BaseTestWithParam<JoinTestConfiguration> {
 public:
  template <typename JoinOperator>
  static std::vector<JoinTestConfiguration> create_configurations() {
    auto configurations = std::vector<JoinTestConfiguration>{};

    /**
     * For each `all_*` set, the first element is used to build the default/base configuration
     * (`JoinTestConfiguration default_configuration`). Thus the first elements are chosen to be sensible defaults.
     */
    auto all_data_types = std::vector<DataType>{};
    hana::for_each(data_type_pairs, [&](auto pair) {
      const DataType d = hana::first(pair);
      all_data_types.emplace_back(d);
    });

    const auto all_predicate_conditions = std::vector{
        PredicateCondition::Equals,      PredicateCondition::NotEquals,
        PredicateCondition::GreaterThan, PredicateCondition::GreaterThanEquals,
        PredicateCondition::LessThan,    PredicateCondition::LessThanEquals,
    };
    const auto all_join_modes = std::vector{JoinMode::Inner,         JoinMode::Left, JoinMode::Right,
                                            JoinMode::FullOuter,     JoinMode::Semi, JoinMode::AntiNullAsFalse,
                                            JoinMode::AntiNullAsTrue};
    const auto all_table_sizes = std::vector{10u, 15u, 0u};
    const auto all_chunk_sizes = std::vector{10u, 3u, 1u};
    const auto all_secondary_predicate_sets = std::vector<std::vector<OperatorJoinPredicate>>{
        {},
        {{{ColumnID{0}, ColumnID{0}}, PredicateCondition::LessThan}},
        {{{ColumnID{0}, ColumnID{0}}, PredicateCondition::GreaterThanEquals}},
        {{{ColumnID{0}, ColumnID{0}}, PredicateCondition::NotEquals}}};
    const auto all_index_sides = std::vector{IndexSide::Left, IndexSide::Right};

    const auto all_input_table_types =
        std::vector{InputTableType::Data, InputTableType::IndividualPosLists, InputTableType::SharedPosList};

    // clang-format off
    JoinTestConfiguration default_configuration{
      InputTableConfiguration{
        InputSide::Left, all_chunk_sizes.front(), all_table_sizes.front(), all_input_table_types.front(), all_encoding_types.front()},  // NOLINT
      InputTableConfiguration{
        InputSide::Right, all_chunk_sizes.front(), all_table_sizes.front(), all_input_table_types.front(), all_encoding_types.front()},  // NOLINT
      JoinMode::Inner,
      DataType::Int,
      DataType::Int,
      true,
      true,
      PredicateCondition::Equals,
      all_secondary_predicate_sets.front(),
      std::make_shared<JoinOperatorFactory<JoinOperator>>(),
      std::nullopt,
      std::nullopt
    };
    // clang-format on

    /**
     * Returns a set of adapted configurations if the join type provides further configuration possibilities that
     * should be tested. Otherwise, a vector containing only the passed configuration is returned.
     */
    const auto build_join_type_specific_variations = [&](const auto& configuration) {
      if constexpr (std::is_same_v<JoinOperator, JoinIndex>) {
        // Currently the Index Join cannot deal with configurations where the join column
        // data types are different, see Issue #2077
        if (configuration.data_type_left != configuration.data_type_right) {
          return std::vector<JoinTestConfiguration>{};
        }

        // For the JoinIndex, additional parameters influence its execution behavior:
        //  - index side (left or right)
        //  - availability of an index for a join column segment
        //  - presence of "single chunk reference guarantee" for join column reference segments

        // share of indexed segments; 1 means all segments are indexed
        std::array<float, 2> indexed_segment_shares{1.0f, .1f};

        std::vector<JoinTestConfiguration> variations{};
        variations.reserve(all_index_sides.size() * indexed_segment_shares.size());
        for (const auto& index_side : all_index_sides) {
          // calculate index chunk counts, eliminate duplicates by using the unordered set
          auto& indexed_input = index_side == IndexSide::Left ? configuration.left_input : configuration.right_input;
          const auto chunk_count = indexed_input.table_size == 0
                                       ? uint32_t{0}
                                       : static_cast<uint32_t>(std::ceil(static_cast<float>(indexed_input.table_size) /
                                                                         static_cast<float>(indexed_input.chunk_size)));

          auto indexed_chunk_ranges = std::set<ChunkRange>{};
          for (const auto indexed_share : indexed_segment_shares) {
            indexed_chunk_ranges.insert(
                ChunkRange{0, static_cast<uint32_t>(std::ceil(indexed_share * static_cast<float>(chunk_count)))});
          }

          for (const auto& indexed_chunk_range : indexed_chunk_ranges) {
            auto variation = configuration;
            auto& variation_indexed_input =
                index_side == IndexSide::Left ? variation.left_input : variation.right_input;
            variation.index_side = index_side;
            variation_indexed_input.indexed_chunk_range = indexed_chunk_range;

            if (variation_indexed_input.table_type != InputTableType::Data && variation.join_mode == JoinMode::Inner) {
              variation_indexed_input.single_chunk_reference_range = indexed_chunk_range;

              if ((indexed_chunk_range.second - indexed_chunk_range.first) > 1) {
                ++variation_indexed_input.single_chunk_reference_range.first;
              }

              if ((chunk_count - indexed_chunk_range.second) > 1) {
                // Leads to the creation of a reference segment that references a single chunk but the referenced
                // data segment is not indexed.
                ++variation_indexed_input.single_chunk_reference_range.second;
              }
            }
            variations.emplace_back(variation);
          }
        }
        variations.shrink_to_fit();
        return variations;
      }
      return std::vector{configuration};
    };

    const auto add_configurations_if_supported = [&](const auto& configuration_candidates) {
      for (const auto& configuration : configuration_candidates) {
        // String vs non-String comparisons are not supported in Hyrise and therefore cannot be tested
        if ((configuration.data_type_left == DataType::String) != (configuration.data_type_right == DataType::String)) {
          return;
        }

        const auto table_type_left =
            (configuration.left_input.table_type == InputTableType::Data ? TableType::Data : TableType::References);
        const auto table_type_right =
            (configuration.right_input.table_type == InputTableType::Data ? TableType::Data : TableType::References);

        JoinConfiguration support_configuration{configuration.join_mode,
                                                configuration.predicate_condition,
                                                configuration.data_type_left,
                                                configuration.data_type_right,
                                                !configuration.secondary_predicates.empty(),
                                                table_type_left,
                                                table_type_right,
                                                configuration.index_side};

        if (JoinOperator::supports(support_configuration)) {
          configurations.emplace_back(configuration);
        }
      }
    };

    // JoinOperators (e.g. JoinHash) might pick a "common type" from the columns used by the primary predicate and cast
    // all values to that (e.g., joining float + long will cast everything to double)
    // Test that this works for all data_type_left/data_type_right combinations
    for (const auto data_type_left : all_data_types) {
      for (const auto data_type_right : all_data_types) {
        auto join_test_configuration = default_configuration;
        join_test_configuration.data_type_left = data_type_left;
        join_test_configuration.data_type_right = data_type_right;

        add_configurations_if_supported(build_join_type_specific_variations(join_test_configuration));
      }
    }

    // JoinOperators (e.g. JoinHash) swap input sides depending on TableSize and JoinMode.
    // Test that predicate_condition and secondary predicates are flipped accordingly
    for (const auto predicate_condition : {PredicateCondition::Equals, PredicateCondition::LessThan}) {
      for (const auto join_mode : all_join_modes) {
        for (const auto left_table_size : all_table_sizes) {
          for (const auto right_table_size : all_table_sizes) {
            for (const auto& secondary_predicates : all_secondary_predicate_sets) {
              auto join_test_configuration = default_configuration;
              join_test_configuration.predicate_condition = predicate_condition;
              join_test_configuration.join_mode = join_mode;
              join_test_configuration.left_input.table_size = left_table_size;
              join_test_configuration.right_input.table_size = right_table_size;
              join_test_configuration.secondary_predicates = secondary_predicates;

              add_configurations_if_supported(build_join_type_specific_variations(join_test_configuration));
            }
          }
        }
      }
    }

    // Anti* joins have different behaviors with NULL values.
    // Also test table sizes, as an empty right input table is a special case where a NULL value from the left side
    // would get emitted.
    for (const auto join_mode : {JoinMode::AntiNullAsTrue, JoinMode::AntiNullAsFalse}) {
      for (const auto left_table_size : all_table_sizes) {
        for (const auto right_table_size : all_table_sizes) {
          auto join_test_configuration = default_configuration;
          join_test_configuration.join_mode = join_mode;
          join_test_configuration.nullable_left = true;
          join_test_configuration.nullable_right = true;
          join_test_configuration.left_input.table_size = left_table_size;
          join_test_configuration.right_input.table_size = right_table_size;

          add_configurations_if_supported(build_join_type_specific_variations(join_test_configuration));
        }
      }
    }

    // JoinOperators need to deal with differently sized Chunks (e.g., smaller last Chunk)
    // Trigger those via testing all table_size/chunk_size combinations
    for (const auto& left_table_size : all_table_sizes) {
      for (const auto& right_table_size : all_table_sizes) {
        for (const auto& chunk_size : all_chunk_sizes) {
          auto join_test_configuration = default_configuration;
          join_test_configuration.left_input.table_size = left_table_size;
          join_test_configuration.right_input.table_size = right_table_size;
          join_test_configuration.left_input.chunk_size = chunk_size;
          join_test_configuration.right_input.chunk_size = chunk_size;

          add_configurations_if_supported(build_join_type_specific_variations(join_test_configuration));
        }
      }
    }

    // Join operators tend to instantiate templates for data types, but also for JoinModes (see probe_semi_anti() in
    // join_hash_steps.hpp) so combinations of those are issued
    for (const auto join_mode : all_join_modes) {
      for (const auto data_type : all_data_types) {
        auto join_test_configuration = default_configuration;
        join_test_configuration.data_type_left = data_type;
        join_test_configuration.data_type_right = data_type;
        join_test_configuration.join_mode = join_mode;

        add_configurations_if_supported(build_join_type_specific_variations(join_test_configuration));
      }
    }

    // Different JoinModes have different handling of NULL values.
    // Additionally, JoinOperators (e.g., JoinSortMerge) have vastly different paths for different PredicateConditions
    // Test all combinations
    for (const auto& join_mode : all_join_modes) {
      for (const auto predicate_condition : all_predicate_conditions) {
        auto join_test_configuration = default_configuration;
        join_test_configuration.join_mode = join_mode;
        join_test_configuration.nullable_left = true;
        join_test_configuration.nullable_right = true;
        join_test_configuration.predicate_condition = predicate_condition;

        add_configurations_if_supported(build_join_type_specific_variations(join_test_configuration));
      }
    }

    // The input tables are designed to have exclusive values (i.e. values not contained in the other tables).
    // Test that these are handled correctly for different JoinModes by swapping the input tables.
    // Additionally, go through all PredicateCondition to test especially the JoinSortMerge's different paths for these
    for (const auto& predicate_condition : all_predicate_conditions) {
      for (const auto& join_mode : all_join_modes) {
        auto join_test_configuration = default_configuration;
        join_test_configuration.join_mode = join_mode;
        join_test_configuration.predicate_condition = predicate_condition;

        join_test_configuration.swap_input_sides();

        add_configurations_if_supported(build_join_type_specific_variations(join_test_configuration));
      }
    }

    // Test all combinations of reference/data input tables. This tests mostly the composition of the output table
    for (const auto table_size : {0u, all_table_sizes.front()}) {
      for (const auto& left_input_table_type : all_input_table_types) {
        for (const auto& right_input_table_type : all_input_table_types) {
          auto join_test_configuration = default_configuration;
          join_test_configuration.left_input.table_type = left_input_table_type;
          join_test_configuration.left_input.table_size = table_size;
          join_test_configuration.right_input.table_type = right_input_table_type;
          join_test_configuration.right_input.table_size = table_size;

          add_configurations_if_supported(build_join_type_specific_variations(join_test_configuration));
        }
      }
    }

    // Test multi-predicate join support for all join modes. Swap the input tables to trigger cases especially in the
    // Anti* modes where a secondary predicate evaluating to FALSE might "save" a tuple from being discarded
    for (const auto& join_mode : all_join_modes) {
      for (const auto& secondary_predicates : all_secondary_predicate_sets) {
        for (const auto& predicate_condition : {PredicateCondition::Equals, PredicateCondition::NotEquals}) {
          for (const auto swap_input_sides : {false, true}) {
            auto join_test_configuration = default_configuration;
            join_test_configuration.join_mode = join_mode;
            join_test_configuration.secondary_predicates = secondary_predicates;
            join_test_configuration.predicate_condition = predicate_condition;

            if (swap_input_sides) {
              join_test_configuration.swap_input_sides();
            }

            add_configurations_if_supported(build_join_type_specific_variations(join_test_configuration));
          }
        }
      }
    }

    // Materialization phases might take advantage of certain encodings. JoinSortedMerge, e.g., has a special path for
    // Dictionaries.
    // Since materialization interacts with data types, NULLs and InputTableTypes, vary all those, too.
    // Use both Equals and LessThan to trigger sorting/non-sorting mode of JoinSortedMerge's ColumnMaterializer.
    for (const auto encoding_type : all_encoding_types) {
      for (const auto data_type : all_data_types) {
        for (const auto nullable : {false, true}) {
          for (const auto table_type : all_input_table_types) {
            for (const auto predicate_condition : {PredicateCondition::Equals, PredicateCondition::LessThan}) {
              auto join_test_configuration = default_configuration;
              join_test_configuration.left_input.encoding_type = encoding_type;
              join_test_configuration.left_input.table_type = table_type;
              join_test_configuration.data_type_left = data_type;
              join_test_configuration.nullable_left = nullable;
              join_test_configuration.right_input.encoding_type = encoding_type;
              join_test_configuration.right_input.table_type = table_type;
              join_test_configuration.data_type_right = data_type;
              join_test_configuration.nullable_right = nullable;
              join_test_configuration.predicate_condition = predicate_condition;

              add_configurations_if_supported(build_join_type_specific_variations(join_test_configuration));
            }
          }
        }
      }
    }

    // JoinHash specific configurations varying the number of radix_bits.
    // The number of radix bits affects the partitioning inside the JoinHash operator, thus table/chunk sizes and
    // different join modes need to be tested in combination, since they all use this partitioning.
    if constexpr (std::is_same_v<JoinOperator, JoinHash>) {
      for (const auto radix_bits : {0, 1, 2, 5}) {
        for (const auto join_mode : {JoinMode::Inner, JoinMode::Right, JoinMode::Semi}) {
          for (const auto left_table_size : all_table_sizes) {
            for (const auto right_table_size : all_table_sizes) {
              for (const auto chunk_size : all_chunk_sizes) {
                for (const auto predicate_condition : {PredicateCondition::Equals, PredicateCondition::NotEquals}) {
                  auto join_test_configuration = default_configuration;
                  join_test_configuration.join_mode = join_mode;
                  join_test_configuration.predicate_condition = predicate_condition;
                  join_test_configuration.left_input.table_size = left_table_size;
                  join_test_configuration.left_input.chunk_size = chunk_size;
                  join_test_configuration.right_input.table_size = right_table_size;
                  join_test_configuration.right_input.chunk_size = chunk_size;
                  join_test_configuration.radix_bits = radix_bits;

                  add_configurations_if_supported(build_join_type_specific_variations(join_test_configuration));
                }
              }
            }
          }
        }
      }
    }

    std::sort(configurations.begin(), configurations.end());
    configurations.erase(std::unique(configurations.begin(), configurations.end()), configurations.end());

    return configurations;
  }

  static std::string get_table_path(const InputTableConfiguration& key) {
    const auto& [side, chunk_size, table_size, input_table_type, encoding_type, indexed_chunk_range,
                 single_chunk_reference_range] = key;

    const auto side_str = side == InputSide::Left ? "left" : "right";
    const auto table_size_str = std::to_string(table_size);

    return "resources/test_data/tbl/join_test_runner/input_table_"s + side_str + "_" + table_size_str + ".tbl";
  }

  static std::shared_ptr<Table> get_table(const InputTableConfiguration& key) {
    auto input_table_iter = input_tables.find(key);
    if (input_table_iter == input_tables.end()) {
      const auto& [side, chunk_size, table_size, input_table_type, encoding_type, indexed_chunk_range,
                   single_chunk_reference_range] = key;
      std::ignore = side;
      std::ignore = table_size;

      auto data_table = load_table(get_table_path(key), chunk_size);

      /**
       * Encode the data table, if requested. Encode only those columns whose DataTypes are supported by the requested
       * encoding.
       */
      if (key.encoding_type != EncodingType::Unencoded) {
        auto chunk_encoding_spec = ChunkEncodingSpec{data_table->column_count()};
        for (auto column_id = ColumnID{0}; column_id < data_table->column_count(); ++column_id) {
          if (encoding_supports_data_type(key.encoding_type, data_table->column_data_type(column_id))) {
            chunk_encoding_spec[column_id] = SegmentEncodingSpec{key.encoding_type};
          } else {
            chunk_encoding_spec[column_id] = SegmentEncodingSpec{EncodingType::Unencoded};
          }
        }
        ChunkEncoder::encode_all_chunks(data_table, chunk_encoding_spec);
      }

      /**
       * Create a Reference-Table pointing 1-to-1 and in-order to the rows in the original data table. This tests the
       * writing of the output table in the JoinOperator, which has to make sure not to create ReferenceSegments
       * pointing to ReferenceSegments. In addition, special index join executions are tested.
       */
      std::shared_ptr<Table> reference_table = nullptr;

      if (input_table_type != InputTableType::Data) {
        reference_table = std::make_shared<Table>(data_table->column_definitions(), TableType::References);

        for (auto chunk_id = ChunkID{0}; chunk_id < data_table->chunk_count(); ++chunk_id) {
          const auto input_chunk = data_table->get_chunk(chunk_id);

          Segments reference_segments;

          if (input_table_type == InputTableType::SharedPosList) {
            const auto pos_list = std::make_shared<RowIDPosList>();
            for (auto chunk_offset = ChunkOffset{0}; chunk_offset < input_chunk->size(); ++chunk_offset) {
              pos_list->emplace_back(RowID{chunk_id, chunk_offset});
            }

            if (chunk_id >= single_chunk_reference_range.first && chunk_id < single_chunk_reference_range.second) {
              pos_list->guarantee_single_chunk();
            }

            for (auto column_id = ColumnID{0}; column_id < data_table->column_count(); ++column_id) {
              reference_segments.emplace_back(std::make_shared<ReferenceSegment>(data_table, column_id, pos_list));
            }

          } else if (input_table_type == InputTableType::IndividualPosLists) {
            for (auto column_id = ColumnID{0}; column_id < data_table->column_count(); ++column_id) {
              const auto pos_list = std::make_shared<RowIDPosList>();
              for (auto chunk_offset = ChunkOffset{0}; chunk_offset < input_chunk->size(); ++chunk_offset) {
                pos_list->emplace_back(RowID{chunk_id, chunk_offset});
              }
              if (chunk_id >= single_chunk_reference_range.first && chunk_id < single_chunk_reference_range.second) {
                pos_list->guarantee_single_chunk();
              }
              reference_segments.emplace_back(std::make_shared<ReferenceSegment>(data_table, column_id, pos_list));
            }
          }

          reference_table->append_chunk(reference_segments);
        }
      }

      /**
       * To sufficiently test IndexJoins, indexes have to be created. Therefore, if index_side is set in the configuration,
       * indexes for the data table are created. The index type is either GroupKeyIndex for dictionary segments or BTreeIndex
       * for non-dictionary segments.
       */

      for (auto chunk_id = indexed_chunk_range.first; chunk_id < indexed_chunk_range.second; ++chunk_id) {
        for (ColumnID column_id{0}; column_id < data_table->column_count(); ++column_id) {
          if (encoding_type == EncodingType::Dictionary) {
            data_table->get_chunk(chunk_id)->create_index<GroupKeyIndex>(std::vector<ColumnID>{column_id});
          } else {
            data_table->get_chunk(chunk_id)->create_index<BTreeIndex>(std::vector<ColumnID>{column_id});
          }
        }
      }

      if (reference_table) {
        input_table_iter = input_tables.emplace(key, reference_table).first;
      } else {
        input_table_iter = input_tables.emplace(key, data_table).first;
      }
    }

    return input_table_iter->second;
  }

  static inline std::map<InputTableConfiguration, std::shared_ptr<Table>> input_tables;
  // Cache reference table to avoid redundant computation of the same
  static inline std::map<JoinTestConfiguration, std::shared_ptr<const Table>> expected_output_tables;
};  // namespace opossum

TEST_P(JoinTestRunner, TestJoin) {
  const auto configuration = GetParam();

  const auto left_input_table = get_table(configuration.left_input);
  const auto right_input_table = get_table(configuration.right_input);

  const auto input_operator_left = std::make_shared<TableWrapper>(left_input_table);
  const auto input_operator_right = std::make_shared<TableWrapper>(right_input_table);

  const auto column_id_left = ColumnID{static_cast<ColumnID::base_type>(
      2 * data_type_order.at(configuration.data_type_left) + (configuration.nullable_left ? 1 : 0))};
  const auto column_id_right = ColumnID{static_cast<ColumnID::base_type>(
      2 * data_type_order.at(configuration.data_type_right) + (configuration.nullable_right ? 1 : 0))};

  const auto primary_predicate =
      OperatorJoinPredicate{{column_id_left, column_id_right}, configuration.predicate_condition};

  const auto join_op = configuration.join_operator_factory->create_operator(input_operator_left, input_operator_right,
                                                                            primary_predicate, configuration);

  // Configuration parameters that are not used by the JoinVerification operator are irrelevant for caching
  auto cached_output_configuration = configuration;

  const auto cleared_input_table_configuration = [](const InputTableConfiguration& input_table_configuration) {
    auto config = input_table_configuration;
    config.chunk_size = {};
    config.table_type = {};
    config.encoding_type = {};
    config.indexed_chunk_range = {};
    config.single_chunk_reference_range = {};
    return config;
  };

  cached_output_configuration.left_input = cleared_input_table_configuration(cached_output_configuration.left_input);
  cached_output_configuration.right_input = cleared_input_table_configuration(cached_output_configuration.right_input);
  cached_output_configuration.join_operator_factory = {};
  cached_output_configuration.radix_bits = {};
  cached_output_configuration.index_side = {};

  auto expected_output_table_iter = expected_output_tables.find(cached_output_configuration);

  const auto join_verification =
      std::make_shared<JoinVerification>(input_operator_left, input_operator_right, configuration.join_mode,
                                         primary_predicate, configuration.secondary_predicates);

  input_operator_left->execute();
  input_operator_right->execute();

  auto actual_table = std::shared_ptr<const Table>{};
  auto expected_table = std::shared_ptr<const Table>{};
  auto table_difference_message = std::optional<std::string>{};

  const auto print_configuration_info = [&]() {
    std::cout << "====================== JoinOperator ========================" << std::endl;
    std::cout << join_op->description(DescriptionMode::MultiLine) << std::endl;
    std::cout << "===================== Left Input Table =====================" << std::endl;
    Print::print(left_input_table, PrintFlags::IgnoreChunkBoundaries);
    std::cout << "Chunk size: " << configuration.left_input.chunk_size << std::endl;
    std::cout << "Table type: " << input_table_type_to_string.at(configuration.left_input.table_type) << std::endl;
    std::cout << "Indexed chunk range: [" << configuration.left_input.indexed_chunk_range.first << ", "
              << configuration.left_input.indexed_chunk_range.second << ")" << std::endl;
    std::cout << "Chunk range with single chunk ref. guarantee: ["
              << configuration.left_input.single_chunk_reference_range.first << ", "
              << configuration.left_input.single_chunk_reference_range.second << ")" << std::endl;
    std::cout << get_table_path(configuration.left_input) << std::endl;
    std::cout << std::endl;
    std::cout << "===================== Right Input Table ====================" << std::endl;
    Print::print(right_input_table, PrintFlags::IgnoreChunkBoundaries);
    std::cout << "Chunk size: " << configuration.right_input.chunk_size << std::endl;
    std::cout << "Table size: " << input_table_type_to_string.at(configuration.right_input.table_type) << std::endl;
    std::cout << "Indexed chunk range: [" << configuration.right_input.indexed_chunk_range.first << ", "
              << configuration.right_input.indexed_chunk_range.second << ")" << std::endl;
    std::cout << "Chunk range with single chunk ref. guarantee: ["
              << configuration.right_input.single_chunk_reference_range.first << ", "
              << configuration.right_input.single_chunk_reference_range.second << ")" << std::endl;
    std::cout << get_table_path(configuration.right_input) << std::endl;
    std::cout << std::endl;
    std::cout << "==================== Actual Output Table ===================" << std::endl;
    if (actual_table) {
      Print::print(actual_table, PrintFlags::IgnoreChunkBoundaries);
      std::cout << std::endl;
    } else {
      std::cout << "No Table produced by the join operator under test" << std::endl;
    }
    std::cout << "=================== Expected Output Table ==================" << std::endl;
    if (expected_table) {
      Print::print(expected_table, PrintFlags::IgnoreChunkBoundaries);
      std::cout << std::endl;
    } else {
      std::cout << "No Table produced by the reference join operator" << std::endl;
    }
    std::cout << "======================== Difference ========================" << std::endl;
    std::cout << *table_difference_message << std::endl;
    std::cout << "============================================================" << std::endl;
  };

  try {
    // Cache reference table to avoid redundant computation of the same
    if (expected_output_table_iter == expected_output_tables.end()) {
      join_verification->execute();
      const auto expected_output_table = join_verification->get_output();
      expected_output_table_iter =
          expected_output_tables.emplace(cached_output_configuration, expected_output_table).first;
    }
    expected_table = expected_output_table_iter->second;

    // Execute the actual join
    join_op->execute();
  } catch (...) {
    // If an error occurred in the join operator under test, we still want to see the test configuration
    print_configuration_info();
    throw;
  }

  actual_table = join_op->get_output();

  table_difference_message = check_table_equal(actual_table, expected_table, OrderSensitivity::No, TypeCmpMode::Strict,
                                               FloatComparisonMode::AbsoluteDifference, IgnoreNullable::No);
  if (table_difference_message) {
    print_configuration_info();
    FAIL();
  }

  if (configuration.index_side) {  // configuration is a specialized JoinIndex configuration
    auto& indexed_input =
        configuration.index_side == IndexSide::Left ? configuration.left_input : configuration.right_input;

    // verify correctness of index usage
    const auto& performance_data = static_cast<const JoinIndex::PerformanceData&>(*join_op->performance_data);

    auto indexed_used_count = indexed_input.indexed_chunk_range.second - indexed_input.indexed_chunk_range.first;

    if (indexed_input.table_type != InputTableType::Data) {
      auto range_begin =
          std::max(indexed_input.indexed_chunk_range.first, indexed_input.single_chunk_reference_range.first);
      auto range_end =
          std::min(indexed_input.indexed_chunk_range.second, indexed_input.single_chunk_reference_range.second);
      indexed_used_count = range_end - range_begin;
    }

    EXPECT_EQ(performance_data.chunks_scanned_with_index, indexed_used_count);
    if (performance_data.chunks_scanned_with_index != indexed_used_count) {
      print_configuration_info();
    }
  }
}

INSTANTIATE_TEST_SUITE_P(JoinNestedLoop, JoinTestRunner,
                         testing::ValuesIn(JoinTestRunner::create_configurations<JoinNestedLoop>()));
INSTANTIATE_TEST_SUITE_P(JoinHash, JoinTestRunner,
                         testing::ValuesIn(JoinTestRunner::create_configurations<JoinHash>()));
INSTANTIATE_TEST_SUITE_P(JoinSortMerge, JoinTestRunner,
                         testing::ValuesIn(JoinTestRunner::create_configurations<JoinSortMerge>()));
INSTANTIATE_TEST_SUITE_P(JoinIndex, JoinTestRunner,
                         testing::ValuesIn(JoinTestRunner::create_configurations<JoinIndex>()));

}  // namespace opossum
