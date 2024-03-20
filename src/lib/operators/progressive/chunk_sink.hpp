#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <semaphore>

#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/progressive_utils.hpp"

namespace hyrise {

// This class needs to be become an operator. It needs an enum of being start/end/in-between.
// Operator allows us to plug it into arbitrary query trees.
class ChunkSink : public AbstractReadOnlyOperator {
 public:
  explicit ChunkSink(const std::shared_ptr<const AbstractOperator>& input_operator, const SinkType sink_type)
      : AbstractReadOnlyOperator(OperatorType::Sink, input_operator), _sink_type{sink_type} {
    // Assert(!input_operator || _sink_type == SinkType::PipelineStart, "Input operator must be set when sink is start of pipeline.");
  }

  void set_name(std::string&& name) {
    _name = name;
  }

  const std::string& name() const override {
    return _name;
  }

  size_t chunk_count() const {
    return _chunks.size();
  }

  /**
   * We take a table to be able to gather the table information of sink-added chunks as we later need this information
   * to create the output table (at least when sink is at end of pipeline).
   * 
   * When we later only allow adding sinks in the LQP/optimization pipeline, this might no longer be necessary as we
   * have the relevant information at hand already.
   */
  void add_chunk(const std::shared_ptr<const Table>& table) {
    Assert(table->chunk_count() == 1, "Add chunk via single-chunk table only.");
    const auto chunk = progressive::recreate_non_const_chunk(table->get_chunk(ChunkID{0}));
    // {
    const auto lock_guard = std::lock_guard<std::mutex>{_chunk_append_mutex};
    _chunks.emplace_back(std::chrono::system_clock::now(), chunk);
    processable_chunks_semaphore.release();
    // }
    // std::cerr << std::format("Sink '{}' added a chunk (#{}) and signaled semaphore.\n", _name, _chunks.size());

    if (!_table_type) {
      // const auto metadata_lock_guard = std::lock_guard<std::mutex>{_metadata_mutex};
      _table_column_definitions = table->column_definitions();
      _table_type = table->type();
    }

    ++_chunks_added;
  }

  // This function is expected to be called sequentially!
  std::shared_ptr<Chunk> pull_chunk(/* const ChunkID chunk_id*/) {
    const auto lock_guard = std::lock_guard<std::mutex>{_chunk_append_mutex};
    Assert(!_last_chunk_has_been_pulled, "One too often pulled.");
    Assert(_sink_type != SinkType::PipelineEnd,
           "Sinks at the end of a pipeline return a table just as normal operators.");

    // We do not let the consumers wait on the semaphore. At least for now. We assume a single proxy operator handles
    // the pulling.
    const auto acquired = processable_chunks_semaphore.try_acquire();
    if (!acquired) {
      // std::cerr << std::format("Sink {} returns nullptr >> all_chunks: {} ... chunks.size(): {} == pulled: {} (added: {}).\n", _name, _all_chunks_have_been_added.load(), _chunks.size(), _chunks_pulled.load(), _chunks_added.load());
      return nullptr;
    }

    const auto chunk_id = _next_chunk_id_to_pull++;

    Assert(!_chunks.empty(), "No chunks to pull.");
    Assert(_next_chunk_id_to_pull.load() == chunk_id + 1,
           "Increment of atomic chunkID counter failed. Really not accessed concurrently?");
    Assert(chunk_id >= 0, "Unexpected chunkID.");

    // Pretty aggressive Assert but should work as we don't pull before semaphore has been signaled.
    Assert(chunk_id < _chunks.size(),
           std::format("chunkID {} not smaller than _chunks.size() of {}.", chunk_id, _chunks.size()));

    ++_chunks_pulled;
    return _chunks[chunk_id].second;
  }

  // Last chunk has been pulled.
  void set_all_chunks_added() {
    _all_chunks_have_been_added = true;
    Assert(_chunks_added == _chunks.size(), "Unexpected.");
  }

  bool finished() /*const*/ {
    const auto lock_guard = std::lock_guard<std::mutex>{_chunk_append_mutex};
    const auto return_value = _all_chunks_have_been_added && _chunks_pulled == _chunks.size();
    // std::cerr << std::format("{} finished()? {} >> all_chunks: {} ... chunks.size(): {} == pulled: {} (added: {}).\n", _name, return_value, _all_chunks_have_been_added.load(), _chunks.size(), _chunks_pulled.load(), _chunks_added.load());
    return return_value;
  }

  std::counting_semaphore<32'768> processable_chunks_semaphore{0};

  // protected:
  std::shared_ptr<const Table> _on_execute() override {
    Assert(_sink_type != SinkType::Forwarding, "on_execute() should not be called on a forwarding sink.");

    if (_sink_type == SinkType::PipelineStart) {
      Assert(left_input()->state() == OperatorState::ExecutedAndAvailable, "Unexpected.");
      const auto& output = left_input()->get_output();
      const auto chunk_count = output->chunk_count();
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        Assert(output->get_chunk(chunk_id)->mvcc_data(), "Unexpected: no mvcc data.");
        auto single_chunk_vector =
            std::vector<std::shared_ptr<Chunk>>{progressive::recreate_non_const_chunk(output->get_chunk(chunk_id))};
        add_chunk(std::make_shared<Table>(output->column_definitions(), output->type(), std::move(single_chunk_vector),
                                          UseMvcc::Yes));
      }

      set_all_chunks_added();
      Assert(_all_chunks_have_been_added, "Something is totally off here.");
      // std::cerr << std::format("Start sink added all {} chunks: set `all_chunks_added`.\n", static_cast<size_t>(chunk_count));

      return output;  // `output` not used, just to conform to the standard operator interface.
    } else if (_sink_type == SinkType::PipelineEnd) {
      // Pipeline end: we wait until the previous operator that is filling the sink has signaled that all chunks have
      // been added.
      std::cerr << "Last sink of pipeline executing.\n";
      while (!_all_chunks_have_been_added) {
        std::cerr << "Waiting\n";
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
      }
      std::cerr << "Sink at end of pipeline is done.\n";

      auto chunk_vector = std::vector<std::shared_ptr<Chunk>>{};
      chunk_vector.reserve(_chunks.size());
      for (const auto& time_chunk_pair : _chunks) {
        chunk_vector.emplace_back(time_chunk_pair.second);
      }
      return std::make_shared<Table>(_table_column_definitions, *_table_type, std::move(chunk_vector));
    }

    Fail("Unexpected SinkType.");
  }

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override {
    return std::make_shared<ChunkSink>(copied_left_input, _sink_type);
  }

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override {}

  SinkType _sink_type;
  std::deque<std::pair<std::chrono::time_point<std::chrono::system_clock>, std::shared_ptr<Chunk>>>
      _chunks{};  // TODO: vector of deques for later partitioning
  std::atomic<bool> _all_chunks_have_been_added{false};
  std::atomic<bool> _last_chunk_has_been_pulled{false};
  std::atomic<uint64_t> _chunks_pulled;
  std::atomic<uint64_t> _chunks_added;
  std::mutex _chunk_append_mutex{};
  std::mutex _metadata_mutex{};
  std::atomic<uint32_t> _next_chunk_id_to_pull{0};

  // To be able to later create the output table from the appended chunks, we store the
  TableColumnDefinitions _table_column_definitions;
  std::optional<TableType> _table_type;

  std::string _name{"Unnamed"};

 private:
};

}  // namespace hyrise