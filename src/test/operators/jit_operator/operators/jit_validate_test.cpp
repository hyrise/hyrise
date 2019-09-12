#include "../../../base_test.hpp"
#include "operators/jit_operator/operators/jit_validate.hpp"
#include "operators/validate.hpp"
#include "storage/chunk.hpp"

namespace opossum {

namespace {

// Mock JitOperator that records whether tuples are passed to it
class MockSink : public AbstractJittable {
 public:
  std::string description() const final { return "MockSink"; }

  void reset() const { _consume_was_called = false; }

  bool consume_was_called() const { return _consume_was_called; }

 private:
  void _consume(JitRuntimeContext& context) const final { _consume_was_called = true; }

  // Must be static, since _consume is const
  static bool _consume_was_called;
};

bool MockSink::_consume_was_called = false;

// Mock JitOperator that passes on individual tuples
class MockSource : public AbstractJittable {
 public:
  std::string description() const final { return "MockSource"; }

  void emit(JitRuntimeContext& context) { _emit(context); }

 private:
  void _consume(JitRuntimeContext& context) const final {}
};

}  // namespace

class JitValidateTest : public BaseTest {
 public:
  JitValidateTest() {
    _test_table = load_table("resources/test_data/tbl/10_ints.tbl", 3u);

    _transaction_context = std::make_shared<TransactionContext>(5u, 3u);

    // data table has three chunks
    {
      auto& mvcc_data = *_test_table->get_chunk(ChunkID(0))->mvcc_data();
      // deleted -> false
      mvcc_data.begin_cids[0] = 1u;
      mvcc_data.end_cids[0] = 2u;
      mvcc_data.tids[0].exchange(0u);
      _expected_values.push_back(false);

      // visible -> true
      mvcc_data.begin_cids[1] = 1u;
      mvcc_data.end_cids[1] = MvccData::MAX_COMMIT_ID;
      mvcc_data.tids[1].exchange(0u);
      _expected_values.push_back(true);

      // not visible for this transaction -> false
      mvcc_data.begin_cids[2] = 10u;
      mvcc_data.end_cids[2] = MvccData::MAX_COMMIT_ID;
      mvcc_data.tids[2].exchange(0u);
      _expected_values.push_back(false);
    }

    {
      auto& mvcc_data = *_test_table->get_chunk(ChunkID(1))->mvcc_data();
      // inserted by other not committed transaction -> false
      mvcc_data.begin_cids[0] = 4u;
      mvcc_data.end_cids[0] = MvccData::MAX_COMMIT_ID;
      mvcc_data.tids[0].exchange(4u);
      _expected_values.push_back(false);

      // inserted by own transaction -> true
      mvcc_data.begin_cids[1] = 5u;
      mvcc_data.end_cids[1] = MvccData::MAX_COMMIT_ID;
      mvcc_data.tids[1].exchange(5u);
      _expected_values.push_back(true);

      // deleted by own transaction -> false
      mvcc_data.begin_cids[2] = 3u;
      mvcc_data.end_cids[2] = 5u;
      mvcc_data.tids[2].exchange(5u);
      _expected_values.push_back(false);
    }

    {
      // deleted by not commited transaction -> true
      auto& mvcc_data = *_test_table->get_chunk(ChunkID(2))->mvcc_data();
      mvcc_data.begin_cids[0] = 1u;
      mvcc_data.end_cids[0] = 4u;
      mvcc_data.tids[0].exchange(4u);
      _expected_values.push_back(true);

      // deleted by commited future transaction -> true
      mvcc_data.begin_cids[1] = 1u;
      mvcc_data.end_cids[1] = 9u;
      mvcc_data.tids[1].exchange(0u);
      _expected_values.push_back(true);
    }
  }

  void validate_row(const ChunkID chunk_id, const ChunkOffset chunk_offset, JitRuntimeContext& context,
                    const bool expected_value, std::shared_ptr<MockSource> source, std::shared_ptr<MockSink> sink,
                    const TableType table_type) {
    if (table_type == TableType::Data) {
      context.mvcc_data = _test_table->get_chunk(chunk_id)->mvcc_data();
      context.row_tids.resize(context.mvcc_data->tids.size());
      auto itr = context.row_tids.begin();
      for (const auto& transaction_id : context.mvcc_data->tids) {
        *itr++ = transaction_id.load();
      }
    }
    context.chunk_offset = chunk_offset;
    sink->reset();
    source->emit(context);
    ASSERT_EQ(sink->consume_was_called(), expected_value);
  }

 protected:
  std::shared_ptr<Table> _test_table;
  std::shared_ptr<TransactionContext> _transaction_context;
  std::vector<bool> _expected_values;
};

TEST_F(JitValidateTest, ValidateOnNonReferenceTable) {
  JitRuntimeContext context;
  context.transaction_id = _transaction_context->transaction_id();
  context.snapshot_commit_id = _transaction_context->snapshot_commit_id();

  auto source = std::make_shared<MockSource>();
  auto validate = std::make_shared<JitValidate>();
  auto sink = std::make_shared<MockSink>();

  // Set table type input in JitValidate
  Table data_table{TableColumnDefinitions{}, TableType::Data};
  std::vector<bool> tuple_non_nullable_information;
  validate->before_specialization(data_table, tuple_non_nullable_information);

  // Link operators to pipeline
  source->set_next_operator(validate);
  validate->set_next_operator(sink);

  auto expected_value_itr = _expected_values.begin();

  validate_row(ChunkID(0), ChunkOffset{0}, context, *expected_value_itr++, source, sink, TableType::Data);
  validate_row(ChunkID(0), ChunkOffset{1}, context, *expected_value_itr++, source, sink, TableType::Data);
  validate_row(ChunkID(0), ChunkOffset{2}, context, *expected_value_itr++, source, sink, TableType::Data);
  validate_row(ChunkID(1), ChunkOffset{0}, context, *expected_value_itr++, source, sink, TableType::Data);
  validate_row(ChunkID(1), ChunkOffset{1}, context, *expected_value_itr++, source, sink, TableType::Data);
  validate_row(ChunkID(1), ChunkOffset{2}, context, *expected_value_itr++, source, sink, TableType::Data);
  validate_row(ChunkID(2), ChunkOffset{0}, context, *expected_value_itr++, source, sink, TableType::Data);
  validate_row(ChunkID(2), ChunkOffset{1}, context, *expected_value_itr++, source, sink, TableType::Data);
}

TEST_F(JitValidateTest, ValidateOnReferenceTable) {
  JitRuntimeContext context;
  context.transaction_id = _transaction_context->transaction_id();
  context.snapshot_commit_id = _transaction_context->snapshot_commit_id();
  context.referenced_table = _test_table;

  auto source = std::make_shared<MockSource>();
  auto validate = std::make_shared<JitValidate>();
  auto sink = std::make_shared<MockSink>();

  // Set table type input in JitValidate
  Table reference_table{TableColumnDefinitions{}, TableType::References};
  std::vector<bool> tuple_non_nullable_information;
  validate->before_specialization(reference_table, tuple_non_nullable_information);

  // Link operators to pipeline
  source->set_next_operator(validate);
  validate->set_next_operator(sink);

  auto expected_value_itr = _expected_values.begin();

  // input reference table has 2 chunks
  auto pos_list = std::make_shared<PosList>(4);
  context.pos_list = pos_list;

  // first chunk
  (*pos_list)[0] = RowID{ChunkID(0), 0u};
  (*pos_list)[1] = RowID{ChunkID(0), 1u};
  (*pos_list)[2] = RowID{ChunkID(0), 2u};
  (*pos_list)[3] = RowID{ChunkID(1), 0u};

  validate_row(ChunkID(0), ChunkOffset{0}, context, *expected_value_itr++, source, sink, TableType::References);
  validate_row(ChunkID(0), ChunkOffset{1}, context, *expected_value_itr++, source, sink, TableType::References);
  validate_row(ChunkID(0), ChunkOffset{2}, context, *expected_value_itr++, source, sink, TableType::References);
  validate_row(ChunkID(0), ChunkOffset{3}, context, *expected_value_itr++, source, sink, TableType::References);

  // second chunk
  (*pos_list)[0] = RowID{ChunkID(1), 1u};
  (*pos_list)[1] = RowID{ChunkID(1), 2u};
  (*pos_list)[2] = RowID{ChunkID(2), 0u};
  (*pos_list)[3] = RowID{ChunkID(2), 1u};

  validate_row(ChunkID(1), ChunkOffset{0}, context, *expected_value_itr++, source, sink, TableType::References);
  validate_row(ChunkID(1), ChunkOffset{1}, context, *expected_value_itr++, source, sink, TableType::References);
  validate_row(ChunkID(1), ChunkOffset{2}, context, *expected_value_itr++, source, sink, TableType::References);
  validate_row(ChunkID(1), ChunkOffset{3}, context, *expected_value_itr++, source, sink, TableType::References);
}

TEST_F(JitValidateTest, UpdateTableTypeInformationBeforeSpecialization) {
  Table data_table{TableColumnDefinitions{}, TableType::Data};
  Table reference_table{TableColumnDefinitions{}, TableType::References};

  JitValidate jit_validate;
  std::vector<bool> tuple_non_nullable_information;

  jit_validate.before_specialization(data_table, tuple_non_nullable_information);
  EXPECT_EQ(jit_validate.input_table_type(), TableType::Data);

  jit_validate.before_specialization(reference_table, tuple_non_nullable_information);
  EXPECT_EQ(jit_validate.input_table_type(), TableType::References);
}

}  // namespace opossum
