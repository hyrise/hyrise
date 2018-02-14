#pragma once

#include <memory>

#include "optimizer/optimizer.hpp"
#include "storage/chunk.hpp"
#include "utils/assert.hpp"

namespace opossum {

class TransactionContext;

/**
 * State shared by the SQLPipeline and its SQLPipelineStatements. E.g., facilitates changing the Optimizer for all
 * SQLPipelineStatements
 */
struct SQLPipelineControlBlock {
  SQLPipelineControlBlock(const ChunkUseMvcc use_mvcc, const std::shared_ptr<TransactionContext>& transaction_context,
                          const std::shared_ptr<Optimizer>& optimizer = Optimizer::get_default_optimizer())
      : use_mvcc(use_mvcc), transaction_context(transaction_context), optimizer(optimizer) {
    Assert(!transaction_context || use_mvcc == ChunkUseMvcc::Yes,
           "Transaction context without MVCC enabled makes no sense");
  }

  // Transaction related
  const ChunkUseMvcc use_mvcc;
  const std::shared_ptr<TransactionContext> transaction_context;

  //
  std::shared_ptr<Optimizer> optimizer;
};

}  // namespace opossum
