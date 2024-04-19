#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/progressive/chunk_sink.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * The shuffle operator is quite some beast:
 *   - it is pipelined (some inputs might come in earlier)
 *   - it might be non-blocking (not sure about that yet). Aspects:
 *     - non-blocking: downstream operators can start earlier, but we lack coordination to yield eventually early
 *                     results when group-by-agg happens
 *     - blocking: as this operation is somewhat nice to parallelize, it should be fast and we can ensure proper result
 *                 coordination for early results
 *   - 
 * 
 * It should run non-blocking even though the purpose of shuffling is somewhat blocking (that means we never have full
 * groups until the operator finished (we don't assume that data is somehow physically pre-partitioned)).
 */

class Shuffle : public AbstractReadOnlyOperator {
 public:
  Shuffle(const std::shared_ptr<const AbstractOperator>& input_operator, std::shared_ptr<ChunkSink>& input_chunk_sink,
          std::shared_ptr<ChunkSink>& output_chunk_sink, std::vector<ColumnID>&& columns,
          std::vector<size_t>&& partition_counts);
  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  std::shared_ptr<ChunkSink>& _input_chunk_sink;
  std::shared_ptr<ChunkSink>& _output_chunk_sink;
  std::vector<ColumnID> _columns{};
  std::vector<size_t> _partition_counts{};
};

}  // namespace hyrise
