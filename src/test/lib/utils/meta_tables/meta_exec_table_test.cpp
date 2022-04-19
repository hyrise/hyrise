#include "base_test.hpp"

#include "operators/table_wrapper.hpp"
#include "utils/meta_tables/meta_exec_table.hpp"

#include "../plugin_test_utils.hpp"

namespace opossum {

class MetaExecTest : public BaseTest {
 protected:
  // void TearDown() override { Hyrise::reset(); }
};

TEST_F(MetaExecTest, IsMutable) {
  const auto meta_exec_table = std::make_shared<MetaExecTable>();

  EXPECT_TRUE(meta_exec_table->can_insert());
  EXPECT_FALSE(meta_exec_table->can_update());
  EXPECT_FALSE(meta_exec_table->can_delete());
}

TEST_F(MetaExecTest, CallUserExecutableFunctions) {
  auto& pm = Hyrise::get().plugin_manager;
  auto& sm = Hyrise::get().storage_manager;

  pm.load_plugin(build_dylib_path("libhyriseTestPlugin"));
  pm.load_plugin(build_dylib_path("libhyriseSecondTestPlugin"));

  SQLPipelineBuilder{
      "INSERT INTO meta_exec (plugin_name, function_name) VALUES ('hyriseTestPlugin', "
      "'OurFreelyChoosableFunctionName')"}
      .create_pipeline()
      .get_result_table();
  // The test plugin creates the below table when the called function is executed
  EXPECT_TRUE(sm.has_table("TableOfTestPlugin"));

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

  // Call non-existing function
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

  // // Call existing, loaded plugin but non-existing function
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

}  // namespace opossum
