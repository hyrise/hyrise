#include "reduce.hpp"

#include <atomic>
#include <memory>
#include <vector>

#include "storage/segment_iterate.hpp"
#include "types.hpp"

namespace {
template <typename DataType>
class MultiplyShiftHasher {
 private:
  uint64_t _multiplier = 0x9E3779B97F4A7C15;  // 64-bit multiplier
  uint32_t _output_size = 44;                 // Output size

 public:
  uint32_t hash(const DataType input) const {
    // if
    // Convert to unsigned 64-bit to handle negative numbers and allow full range
    uint64_t input_unsigned = 0;
    if constexpr (std::is_same_v<DataType, int32_t>) {
      input_unsigned = static_cast<uint64_t>(input);
    } else {
      Fail("aahh");
    }

    // Multiply (result is 64-bit)
    const auto product = (input_unsigned * _multiplier) ^ (input_unsigned >> 33);
    // For full 64-bit output, no shift needed if m=64
    return product >> (64 - _output_size);
  }

  std::pair<uint32_t, uint32_t> hash22(const DataType input) const {
    uint32_t hash_44bit = hash(input);
    uint32_t upper_22 = hash_44bit >> 22;
    uint32_t lower_22 = hash_44bit & ((1U << 22) - 1);
    return {upper_22, lower_22};
  }
};
}  // namespace

namespace hyrise {

Reduce::Reduce(const std::shared_ptr<const AbstractOperator>& input_relation,
               const std::shared_ptr<const AbstractOperator>& input_filter)
    : AbstractReadOnlyOperator{OperatorType::Reduce, input_relation, input_filter} {}

void Reduce::_create_filter(const ColumnID column_id, const uint32_t filter_size) {
  _filter = std::make_shared<std::vector<std::atomic_uint64_t>>(filter_size);

  const auto input_table = left_input_table();
  const auto chunk_count = input_table->chunk_count();

  resolve_data_type(input_table->column_data_type(column_id), [&](const auto column_data_type) {
    using ColumnDataType = typename decltype(column_data_type)::type;
    auto hasher = MultiplyShiftHasher<ColumnDataType>{};

    for (auto chunk_index = ChunkID{0}; chunk_index < chunk_count; ++chunk_index) {
      const auto& segment = input_table->get_chunk(chunk_index)->get_segment(column_id);

      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
        if (!position.is_null()) {
          const auto hash = hasher.hash(position.value());
          const auto hashes = hasher.hash22(position.value());
          std::cout << position.value() << " : " << hash << " : " << hashes.first << " : " << hashes.second << std::endl;
          _set_bit(hashes.first);
          _set_bit(hashes.second);
        }
      });
    }
  });
}

const std::string& Reduce::name() const {
  static const auto name = std::string{"Reduce"};
  return name;
}

const std::shared_ptr<std::vector<std::atomic_uint64_t>>& Reduce::export_filter() const {
  return _filter;
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
    size_t index = hash_22bit / 64;       // Determine the block
    size_t offset = hash_22bit % 64;      // Determine the bit within the block
    // std::cout << "Set bit " << offset << " at index " << index << ".\n";
    (*_filter)[index].fetch_or(1ULL << offset, std::memory_order_relaxed);
  }

  // Atomically get the bit
  bool Reduce::_get_bit(const uint32_t hash_22bit) const {
    size_t index = hash_22bit / 64;       // Determine the block
    size_t offset = hash_22bit % 64;      // Determine the bit within the block
    return ((*_filter)[index].load(std::memory_order_relaxed) >> offset) & 1;
  }

  // Atomically clear the bit
  void Reduce::_clear_bit(const uint32_t hash_22bit) {
    size_t index = hash_22bit / 64;       // Determine the block
    size_t offset = hash_22bit % 64;      // Determine the bit within the block
    (*_filter)[index].fetch_and(~(1ULL << offset), std::memory_order_relaxed);
  }

}  // namespace hyrise
