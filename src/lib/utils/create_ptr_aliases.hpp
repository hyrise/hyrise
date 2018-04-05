#pragma once

#include <memory>

#define CREATE_PTR_ALIASES(name) \
  using name##SPtr = std::shared_ptr<name>; \
  using name##CSPtr = std::shared_ptr<const name>; \
  using name##WPtr = std::weak_ptr<name>; \
  using name##CWPtr = std::weak_ptr<const name>;

#define CREATE_TEMPLATE_PTR_ALIASES(name) \
  template<typename T> using name##SPtr = std::shared_ptr<name<T>>; \
  template<typename T> using name##CSPtr = std::shared_ptr<const name<T>>; \
  template<typename T> using name##WPtr = std::weak_ptr<name<T>>; \
  template<typename T> using name##CWPtr = std::weak_ptr<const name<T>>;

namespace opossum {
class TableStatistics;
class BaseCompressedVector;
class PQPExpression;
class DerivedExpression;
class ColumnVisitableContext;
class Table;
class TaskQueue;
class SQLPipelineStatement;
class AbstractLQPNode;
class CommitContext;
class TransactionContext;
class JoinEdge;
class ChunkColumnStatistics;
class Topology;
class ARTNode;
class AbstractScheduler;
class AbstractOperator;
class ChunkStatistics;
class JoinGraph;
class Worker;
class JobTask;
class BaseColumn;
class OperatorTask;
class ProcessingUnit;
class BaseColumnStatistics;
class Optimizer;
class Base;
class AbstractTask;
class ProjectionNode;
class BaseIndex;
class BaseEncodedColumn;
class LQPExpression;
class Chunk;
struct MvccColumns;
struct ChunkAccessCounter;
class BaseJitColumnReader;
class BaseJitColumnWriter;
class AbstractJittable;
class JitExpression;
class AbstractReadWriteOperator;
class BaseValueColumn;
class PredicateNode;
class ColumnVisitable;
class Delete;
class Insert;
class TableScan;
class UidAllocator;
class AbstractJoinPlanPredicate;
class AbstractFilter;
class AbstractRule;
class JoinNode;
class UnionNode;
struct OutputPacket;

template<typename> class ValueColumn;
template<typename> class DictionaryColumn;
template<typename> class SQLQueryCache;

CREATE_PTR_ALIASES(OutputPacket)
CREATE_PTR_ALIASES(JoinNode)
CREATE_PTR_ALIASES(UnionNode)
CREATE_PTR_ALIASES(AbstractFilter)
CREATE_PTR_ALIASES(AbstractRule)
CREATE_PTR_ALIASES(AbstractJoinPlanPredicate)
CREATE_PTR_ALIASES(UidAllocator)
CREATE_PTR_ALIASES(TableScan)
CREATE_PTR_ALIASES(Insert)
CREATE_PTR_ALIASES(Delete)
CREATE_PTR_ALIASES(ColumnVisitable)
CREATE_PTR_ALIASES(PredicateNode)
CREATE_PTR_ALIASES(BaseValueColumn)
CREATE_PTR_ALIASES(AbstractReadWriteOperator)
CREATE_PTR_ALIASES(TableStatistics)
CREATE_PTR_ALIASES(BaseCompressedVector)
CREATE_PTR_ALIASES(PQPExpression)
CREATE_PTR_ALIASES(DerivedExpression)
CREATE_PTR_ALIASES(ColumnVisitableContext)
CREATE_PTR_ALIASES(Table)
CREATE_PTR_ALIASES(TaskQueue)
CREATE_PTR_ALIASES(SQLPipelineStatement)
CREATE_PTR_ALIASES(AbstractLQPNode)
CREATE_PTR_ALIASES(CommitContext)
CREATE_PTR_ALIASES(TransactionContext)
CREATE_PTR_ALIASES(JoinEdge)
CREATE_PTR_ALIASES(ChunkColumnStatistics)
CREATE_PTR_ALIASES(Topology)
CREATE_PTR_ALIASES(ARTNode)
CREATE_PTR_ALIASES(AbstractScheduler)
CREATE_PTR_ALIASES(AbstractOperator)
CREATE_PTR_ALIASES(ChunkStatistics)
CREATE_PTR_ALIASES(JoinGraph)
CREATE_PTR_ALIASES(Worker)
CREATE_PTR_ALIASES(JobTask)
CREATE_PTR_ALIASES(BaseColumn)
CREATE_PTR_ALIASES(OperatorTask)
CREATE_PTR_ALIASES(ProcessingUnit)
CREATE_PTR_ALIASES(BaseColumnStatistics)
CREATE_PTR_ALIASES(MvccColumns)
CREATE_PTR_ALIASES(ChunkAccessCounter)
CREATE_PTR_ALIASES(Optimizer)
CREATE_PTR_ALIASES(Base)
CREATE_PTR_ALIASES(OutputPacket)
CREATE_PTR_ALIASES(AbstractTask)
CREATE_PTR_ALIASES(ProjectionNode)
CREATE_PTR_ALIASES(BaseIndex)
CREATE_PTR_ALIASES(BaseEncodedColumn)
CREATE_PTR_ALIASES(LQPExpression)
CREATE_PTR_ALIASES(Chunk)
CREATE_PTR_ALIASES(BaseJitColumnReader)
CREATE_PTR_ALIASES(BaseJitColumnWriter)
CREATE_PTR_ALIASES(AbstractJittable)
CREATE_PTR_ALIASES(JitExpression)

CREATE_TEMPLATE_PTR_ALIASES(DictionaryColumn)
CREATE_TEMPLATE_PTR_ALIASES(ValueColumn)
CREATE_TEMPLATE_PTR_ALIASES(SQLQueryCache)
}