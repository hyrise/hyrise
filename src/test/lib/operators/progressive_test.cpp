#include "base_test.hpp"
#include "operators/progressive/chunk_sink.hpp"
#include "operators/progressive/shuffle.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "types.hpp"
#include "utils/progressive_utils.hpp"

namespace hyrise {

class ProgressiveTest : public BaseTest {};

TEST(ProgressiveTest, Shuffle) {
  // Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  // auto table = load_table("resources/test_data/tbl/tpch/sf-0.02/lineitem.tbl", ChunkOffset{2});

  // auto table_wrapper = std::make_shared<TableWrapper>(table);
  // table_wrapper->never_clear_output();
  // table_wrapper->execute();

  // auto sink1 = std::make_shared<ChunkSink>(table_wrapper, SinkType::PipelineStart);
  // auto sink2 = std::make_shared<ChunkSink>(table_wrapper, SinkType::PipelineEnd);

  // // Populate first chunk exchange.
  // for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
  //   auto single_chunk_vector = std::vector<std::shared_ptr<Chunk>>{};
  //   single_chunk_vector.emplace_back(progressive::recreate_non_const_chunk(table->get_chunk(chunk_id)));
  //   sink1->add_chunk(std::make_shared<Table>(table->column_definitions(), TableType::Data, std::move(single_chunk_vector), UseMvcc::Yes));
  // }
  // sink1->set_all_chunks_added();

  // auto shuffle = std::make_shared<Shuffle>(table_wrapper, sink1, sink2, std::vector<ColumnID>{ColumnID{0}}, std::vector<size_t>{size_t{64}});
  // shuffle->execute();
}

}  // namespace hyrise
