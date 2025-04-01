#include "reduce.hpp"

#include <atomic>
#include <memory>
#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <filesystem>

#include "operators/abstract_operator.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "hyrise.hpp"

namespace {

template <typename T, uint32_t p = 16>
class Hasher {
 public:
  using Ty = typename std::remove_cv<T>::type;

  uint64_t _multiplier = 0x9E3779B97F4A7C15;

  uint32_t hash0(const Ty& key) {
    uint32_t knuth = 596572387u;  // Peter 1

    uint32_t casted_key = 0;
    if constexpr (std::is_same_v<Ty, int32_t>) {
      casted_key = static_cast<uint32_t>(key) + 1;
    } else {
      Fail("Unsupported type.");
    }

    return (casted_key * knuth) >> (32 - p);
  }

  uint32_t hash1(const Ty& key) {
    uint32_t knuth = 370248451u;  // Peter 1

    uint32_t casted_key = 0;
    if constexpr (std::is_same_v<Ty, int32_t>) {
      casted_key = static_cast<uint32_t>(key) + 1;
    } else {
      Fail("Unsupported type.");
    }

    return (casted_key * knuth) >> (32 - p);
  }

  uint32_t hash2(const Ty& key) {
    uint32_t knuth = 2654435769u;  // Peter 1

    uint32_t casted_key = 0;
    if constexpr (std::is_same_v<Ty, int32_t>) {
      casted_key = static_cast<uint32_t>(key) + 1;
    } else {
      Fail("Unsupported type.");
    }

    return (casted_key * knuth) >> (32 - p);
  }
};

}  // namespace

namespace hyrise {

Reduce::Reduce(const std::shared_ptr<const AbstractOperator>& left_input,
               const std::shared_ptr<const AbstractOperator>& right_input, const OperatorJoinPredicate predicate,
               const bool update_filter)
    : AbstractReadOnlyOperator{OperatorType::Reduce, left_input, right_input},
      _predicate(predicate),
      _update_filter(update_filter) {}

void Reduce::_create_filter(const std::shared_ptr<const Table>& table, const ColumnID column_id) {
  uint32_t FILTER_SIZE;
  const char* env = std::getenv("SIZE");
  if (env) {
    _filter_size_string = std::string{env};
    FILTER_SIZE = static_cast<uint32_t>(pow(2u, std::stoi(_filter_size_string)));
  } else {
    Fail("Missing filter size.");
  }


  Assert(FILTER_SIZE % 64 == 0, "Filter size must be a multiple of 64.");
  _filter = std::make_shared<std::vector<std::atomic_uint64_t>>(FILTER_SIZE / 64);

  const auto chunk_count = table->chunk_count();

  resolve_data_type(table->column_data_type(column_id), [&](const auto column_data_type) {
    using ColumnDataType = typename decltype(column_data_type)::type;
    auto hasher = Hasher<ColumnDataType>{};

    for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; ++chunk_index) {
      const auto& segment = table->get_chunk(chunk_index)->get_segment(column_id);

      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
        if (!position.is_null()) {

          const char* env = std::getenv("HASH");
          if (env) {
            _hash_count = std::string{env};
            if (_hash_count == "1") {
              _set_bit(hasher.hash0(position.value()));
            } else if (_hash_count == "2") {
              _set_bit(hasher.hash0(position.value()));
              _set_bit(hasher.hash1(position.value()));
            } else if (_hash_count == "3") {
              _set_bit(hasher.hash0(position.value()));
              _set_bit(hasher.hash1(position.value()));
              _set_bit(hasher.hash2(position.value()));
            } else {
              Fail("Invalid hash count.");
            }
          } else {
            Fail("Missing hash count.");
          }
        }
      });
    }
  });
}

std::shared_ptr<Table> Reduce::_create_reduced_table() {
  Assert(_filter, "Can not filter without filter.");

  const auto input_table = left_input_table();
  const auto chunk_count = input_table->chunk_count();
  const auto column_id = _predicate.column_ids.first;

  // std::cout << "Input row count: " << input_table->row_count() << std::endl;

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{};
  output_chunks.reserve(chunk_count);

  resolve_data_type(input_table->column_data_type(column_id), [&](const auto column_data_type) {
    using ColumnDataType = typename decltype(column_data_type)::type;
    auto hasher = Hasher<ColumnDataType>{};

    for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; ++chunk_index) {
      const auto& chunk = input_table->get_chunk(chunk_index);
      const auto& segment = chunk->get_segment(column_id);
      auto matches = std::make_shared<RowIDPosList>();
      matches->guarantee_single_chunk();

      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
        if (!position.is_null()) {
          bool true0 = true;
          bool true1 = true;
          bool true2 = true;

            if (_hash_count == "1") {
              true0 = _get_bit(hasher.hash0(position.value()));
            } else if (_hash_count == "2") {
              true0 = _get_bit(hasher.hash0(position.value()));
              true1 = _get_bit(hasher.hash1(position.value()));
            } else if (_hash_count == "3") {
              true0 = _get_bit(hasher.hash0(position.value()));
              true1 = _get_bit(hasher.hash1(position.value()));
              true2 = _get_bit(hasher.hash2(position.value()));
            }

          if (true0 && true1 && true2) {
            matches->emplace_back(RowID{chunk_index, position.chunk_offset()});
          }
        }
      });

      if (!matches->empty()) {
        const auto column_count = input_table->column_count();
        auto out_segments = Segments{};
        out_segments.reserve(column_count);

        /**
       * matches contains a list of row IDs into this chunk. If this is not a reference table, we can directly use
       * the matches to construct the reference segments of the output. If it is a reference segment, we need to
       * resolve the row IDs so that they reference the physical data segments (value, dictionary) instead, since we
       * don’t allow multi-level referencing. To save time and space, we want to share position lists between segments
       * as much as possible. Position lists can be shared between two segments iff (a) they point to the same table
       * and (b) the reference segments of the input table point to the same positions in the same order (i.e. they
       * share their position list).
       */
        auto keep_chunk_sort_order = true;
        if (input_table->type() == TableType::References) {
          if (matches->size() == chunk->size()) {
            // Shortcut - the entire input reference segment matches, so we can simply forward that chunk
            for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
              const auto segment_in = chunk->get_segment(column_id);
              out_segments.emplace_back(segment_in);
            }
          } else {
            auto filtered_pos_lists = std::map<std::shared_ptr<const AbstractPosList>, std::shared_ptr<RowIDPosList>>{};

            for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
              const auto segment_in = chunk->get_segment(column_id);

              auto ref_segment_in = std::dynamic_pointer_cast<const ReferenceSegment>(segment_in);
              DebugAssert(ref_segment_in, "All segments should be of type ReferenceSegment.");

              const auto pos_list_in = ref_segment_in->pos_list();

              const auto table_out = ref_segment_in->referenced_table();
              const auto column_id_out = ref_segment_in->referenced_column_id();

              auto& filtered_pos_list = filtered_pos_lists[pos_list_in];

              if (!filtered_pos_list) {
                filtered_pos_list = std::make_shared<RowIDPosList>(matches->size());
                if (pos_list_in->references_single_chunk()) {
                  filtered_pos_list->guarantee_single_chunk();
                } else {
                  // When segments reference multiple chunks, we do not keep the sort order of the input chunk. The main
                  // reason is that several table scan implementations split the pos lists by chunks (see
                  // AbstractDereferencedColumnTableScanImpl::_scan_reference_segment) and thus shuffle the data. While
                  // this does not affect all scan implementations, we chose the safe and defensive path for now.
                  keep_chunk_sort_order = false;
                }

                auto offset = size_t{0};
                for (const auto& match : *matches) {
                  const auto row_id = (*pos_list_in)[match.chunk_offset];
                  (*filtered_pos_list)[offset] = row_id;
                  ++offset;
                }
              }

              const auto ref_segment_out =
                  std::make_shared<ReferenceSegment>(table_out, column_id_out, filtered_pos_list);
              out_segments.push_back(ref_segment_out);
            }
          }
        } else {
          matches->guarantee_single_chunk();

          // If the entire chunk is matched, create an EntireChunkPosList instead
          const auto output_pos_list = matches->size() == chunk->size()
                                           ? static_cast<std::shared_ptr<AbstractPosList>>(
                                                 std::make_shared<EntireChunkPosList>(chunk_index, chunk->size()))
                                           : static_cast<std::shared_ptr<AbstractPosList>>(matches);

          for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
            const auto ref_segment_out = std::make_shared<ReferenceSegment>(input_table, column_id, output_pos_list);
            out_segments.push_back(ref_segment_out);
          }
        }

        const auto out_chunk = std::make_shared<Chunk>(out_segments, nullptr, chunk->get_allocator());
        out_chunk->set_immutable();
        if (keep_chunk_sort_order && !chunk->individually_sorted_by().empty()) {
          out_chunk->set_individually_sorted_by(chunk->individually_sorted_by());
        }
        output_chunks.emplace_back(out_chunk);
      }
    }
  });


  const auto output_table = std::make_shared<Table>(input_table->column_definitions(), TableType::References, std::move(output_chunks));
  // std::cout << "Output row count: " << output_table->row_count() << std::endl;
  

  auto file_exists = std::filesystem::exists("reduction_stats.csv");
  std::ofstream output_file;
  output_file.open("reduction_stats.csv", std::ios_base::app);

  if (!file_exists) {
        output_file << "filter_size,hash_count,benchmark,query,input_count,output_count\n";
  }
  output_file << _filter_size_string << ","
              << _hash_count << ","
              << Hyrise::get().benchmark_name << ","
              << Hyrise::get().query_name << ","
              << input_table->row_count() << ","
              << output_table->row_count() << "\n";

  return output_table;
}

const std::string& Reduce::name() const {
  static const auto name = std::string{"Reduce"};
  return name;
}

const std::shared_ptr<std::vector<std::atomic_uint64_t>>& Reduce::export_filter() const {
  Assert(_filter, "Filter is not set, reducer was probably not executed.");
  return _filter;
}

std::shared_ptr<const Table> Reduce::_on_execute() {
  if (_right_input->type() == OperatorType::Reduce) {
    const auto input_reducer = std::static_pointer_cast<const Reduce>(_right_input);
    _filter = input_reducer->export_filter();
  } else {
    _create_filter(_right_input->get_output(), _predicate.column_ids.second);
  }
  const auto output_table = _create_reduced_table();

  if (_update_filter) {
    _filter = nullptr;
    _create_filter(output_table, _predicate.column_ids.first);
  }

  return output_table;
}

void Reduce::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<AbstractOperator> Reduce::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>&  /*copied_ops*/) const {
  return std::make_shared<Reduce>(copied_left_input, copied_right_input, _predicate, _update_filter);
}

void Reduce::_set_bit(const uint32_t hash_22bit) {
  size_t index = hash_22bit / 64;   // Determine the block
  size_t offset = hash_22bit % 64;  // Determine the bit within the block
  (*_filter)[index].fetch_or(1ULL << offset, std::memory_order_relaxed);
}

bool Reduce::_get_bit(const uint32_t hash_22bit) const {
  size_t index = hash_22bit / 64;   // Determine the block
  size_t offset = hash_22bit % 64;  // Determine the bit within the block
  return ((*_filter)[index].load(std::memory_order_relaxed) >> offset) & 1;
}

}  // namespace hyrise
