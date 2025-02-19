#include "reduce.hpp"

#include <atomic>
#include <memory>
#include <vector>

#include "storage/segment_iterate.hpp"
#include "types.hpp"

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

Reduce::Reduce(const std::shared_ptr<const AbstractOperator>& input_relation,
               const std::shared_ptr<const AbstractOperator>& input_filter)
    : AbstractReadOnlyOperator{OperatorType::Reduce, input_relation, input_filter} {}

void Reduce::_create_filter(const ColumnID column_id, const uint32_t filter_size) {
  Assert(filter_size % 64 == 0, "Filter size must be a multiple of 64.");
  _filter = std::make_shared<std::vector<std::atomic_uint64_t>>(filter_size / 64);

  const auto input_table = left_input_table();
  const auto chunk_count = input_table->chunk_count();

  resolve_data_type(input_table->column_data_type(column_id), [&](const auto column_data_type) {
    using ColumnDataType = typename decltype(column_data_type)::type;
    auto hasher = Hasher<ColumnDataType>{};

    for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; ++chunk_index) {
      const auto& segment = input_table->get_chunk(chunk_index)->get_segment(column_id);

      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
        if (!position.is_null()) {
          const auto hash0 = hasher.hash0(position.value());
          const auto hash1 = hasher.hash1(position.value());
          const auto hash2 = hasher.hash2(position.value());
          // std::cout << position.value() << " : " << hash0 << " " << hash1 << " " << hash2 << std::endl;
          _set_bit(hash0);
          _set_bit(hash1);
          _set_bit(hash2);
        }
      });
    }
  });
}

std::shared_ptr<Table> Reduce::_execute_filter(const ColumnID column_id) {
  if (_filter == nullptr) {
    Fail("Can not filter without filter.");
  }
  // Assert(_filter, "Can not filter without filter.");

  const auto input_table = left_input_table();
  const auto chunk_count = input_table->chunk_count();

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
          const auto hash0 = hasher.hash0(position.value());
          const auto hash1 = hasher.hash1(position.value());
          const auto hash2 = hasher.hash2(position.value());
          
          if (_get_bit(hash0) && _get_bit(hash1) && _get_bit(hash2)) {
            matches->emplace_back(RowID{chunk_index, position.chunk_offset()});
          }
        }
      });

      if (!matches->empty()) {
        const auto column_count = input_table->column_count();
        auto out_segments = Segments{};
        out_segments.reserve(column_count);

        for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
          const auto ref_segment_out = std::make_shared<ReferenceSegment>(input_table, column_id, matches);
          out_segments.push_back(ref_segment_out);
        }

        const auto output_chunk = std::make_shared<Chunk>(out_segments, nullptr, chunk->get_allocator());
        output_chunk->set_immutable();

        output_chunks.emplace_back(output_chunk);
      }

    }
  });

  return std::make_shared<Table>(input_table->column_definitions(), TableType::References, std::move(output_chunks));
}

const std::string& Reduce::name() const {
  static const auto name = std::string{"Reduce"};
  return name;
}

const std::shared_ptr<std::vector<std::atomic_uint64_t>>& Reduce::export_filter() const {
  return _filter;
}

void Reduce::import_filter(const std::shared_ptr<std::vector<std::atomic_uint64_t>>& filter) {
  Assert(!_filter, "Filter must not exist.");
  _filter = filter;
}

std::shared_ptr<const Table> Reduce::_on_execute() {
  return left_input_table();
}

void Reduce::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<AbstractOperator> Reduce::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return nullptr;
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
