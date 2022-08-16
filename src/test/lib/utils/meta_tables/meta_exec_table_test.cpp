#include "base_test.hpp"

#include "operators/table_wrapper.hpp"
#include "utils/meta_tables/meta_exec_table.hpp"

#include "../plugin_test_utils.hpp"

namespace hyrise {

class MetaExecTest : public BaseTest {};

TEST_F(MetaExecTest, IsMutable) {
  const auto meta_exec_table = std::make_shared<MetaExecTable>();

  EXPECT_TRUE(meta_exec_table->can_insert());
  EXPECT_FALSE(meta_exec_table->can_update());
  EXPECT_FALSE(meta_exec_table->can_delete());
}

TEST_F(MetaExecTest, SelectUserExecutableFunctions) {
  auto& pm = Hyrise::get().plugin_manager;

  pm.load_plugin(build_dylib_path("libhyriseTestPlugin"));
  pm.load_plugin(build_dylib_path("libhyriseSecondTestPlugin"));

  const auto expected_table = std::make_shared<Table>(
      TableColumnDefinitions{{"plugin_name", DataType::String, false}, {"function_name", DataType::String, false}},
      TableType::Data, ChunkOffset{5});

  expected_table->append({pmr_string{"hyriseSecondTestPlugin"}, pmr_string{"OurFreelyChoosableFunctionName"}});
  expected_table->append({pmr_string{"hyriseTestPlugin"}, pmr_string{"OurFreelyChoosableFunctionName"}});
  expected_table->append({pmr_string{"hyriseTestPlugin"}, pmr_string{"SpecialFunction17"}});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(expected_table));
  table_wrapper->execute();

  const auto& [_, result_table] = SQLPipelineBuilder{"SELECT * FROM meta_exec"}.create_pipeline().get_result_table();
  EXPECT_TABLE_EQ_UNORDERED(result_table, table_wrapper->get_output());
}

TEST_F(MetaExecTest, CallUserExecutableFunctions) {
  auto& pm = Hyrise::get().plugin_manager;
  auto& sm = Hyrise::get().storage_manager;

  pm.load_plugin(build_dylib_path("libhyriseTestPlugin"));
  pm.load_plugin(build_dylib_path("libhyriseSecondTestPlugin"));

  // Test plugin has state and cretes tables with an increasing id.
  const auto exec_query =
      "INSERT INTO meta_exec (plugin_name, function_name) VALUES ('hyriseTestPlugin', "
      "'OurFreelyChoosableFunctionName')";
  SQLPipelineBuilder{exec_query}.create_pipeline().get_result_table();
  // The test plugin creates the below table when the called function is executed
  EXPECT_TRUE(sm.has_table("TableOfTestPlugin_0"));

  SQLPipelineBuilder{exec_query}.create_pipeline().get_result_table();
  EXPECT_TRUE(sm.has_table("TableOfTestPlugin_1"));

  SQLPipelineBuilder{
      "INSERT INTO meta_exec (plugin_name, function_name) VALUES ('hyriseSecondTestPlugin', "
      "'OurFreelyChoosableFunctionName')"}
      .create_pipeline()
      .get_result_table();
  // The second test plugin creates the below table when the called function is executed
  EXPECT_TRUE(sm.has_table("TableOfSecondTestPlugin"));
}

TEST_F(MetaExecTest, CallNotCallableUserExecutableFunctions) {
  auto& pm = Hyrise::get().plugin_manager;

  // We have to manually rollback the transaction contexts below because otherwise their destruction would cause an
  // exeception to be thrown. This is due to an assert in the TransactionContext's destructor checking for failed
  // operators. See ~TransactionContext for details.

  // Call non-existing plugin (with non-existing function)
  {
    auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    auto sql_pipeline =
        SQLPipelineBuilder{
            "INSERT INTO meta_exec (plugin_name, function_name) VALUES ('hyriseUnknownPlugin', "
            "'OurFreelyChoosableFunctionName')"}
            .with_transaction_context(transaction_context)
            .create_pipeline();
    EXPECT_THROW(sql_pipeline.get_result_table(), std::logic_error);
    sql_pipeline.transaction_context()->rollback(RollbackReason::Conflict);
  }

  // Call existing, loaded plugin but non-existing function
  pm.load_plugin(build_dylib_path("libhyriseSecondTestPlugin"));

  {
    auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    auto sql_pipeline =
        SQLPipelineBuilder{
            "INSERT INTO meta_exec (plugin_name, function_name) VALUES ('hyriseSecondTestPlugin', 'SpecialFunction17')"}
            .with_transaction_context(transaction_context)
            .create_pipeline();
    EXPECT_THROW(sql_pipeline.get_result_table(), std::logic_error);
    sql_pipeline.transaction_context()->rollback(RollbackReason::Conflict);
  }

  // Call function exposed by plugin but plugin has been unloaded before
  pm.unload_plugin("hyriseSecondTestPlugin");

  {
    auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    auto sql_pipeline =
        SQLPipelineBuilder{
            "INSERT INTO meta_exec (plugin_name, function_name) VALUES ('hyriseSecondTestPlugin', "
            "'OurFreelyChoosableFunctionName')"}
            .with_transaction_context(transaction_context)
            .create_pipeline();
    EXPECT_THROW(sql_pipeline.get_result_table(), std::logic_error);
    sql_pipeline.transaction_context()->rollback(RollbackReason::Conflict);
  }
}

}  // namespace hyrise
