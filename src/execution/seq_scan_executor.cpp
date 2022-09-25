//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_iter_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {}

void SeqScanExecutor::TupleSchemaTranformUseEvaluate(const Tuple *table_tuple, const Schema *table_schema,
                                                     Tuple *dest_tuple, const Schema *dest_schema) {
  auto cloumns = dest_schema->GetColumns();
  std::vector<Value> values;
  values.reserve(cloumns.size());

  for (const auto &column : cloumns) {
    values.emplace_back(column.GetExpr()->Evaluate(table_tuple, table_schema));
  }
  *dest_tuple = Tuple(values, dest_schema);
}

bool SeqScanExecutor::SchemaEqual(const Schema *table_schema, const Schema *output_schema) {
  auto table_columns = table_schema->GetColumns();
  auto output_columns = output_schema->GetColumns();

  if (table_columns.size() != output_columns.size()) {
    return false;
  }

  int schema_size = table_columns.size();
  uint32_t offset1;
  uint32_t offset2;
  std::string name1;
  std::string name2;

  for (int i = 0; i < schema_size; i++) {
    name1 = table_columns[i].GetName();
    name2 = output_columns[i].GetName();
    offset1 = table_columns[i].GetOffset();
    offset2 = output_columns[i].GetOffset();
    if (offset1 != offset2 || name1 != name2) {
      return false;
    }
  }

  return true;
}

void SeqScanExecutor::Init() {
  auto table_oid = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());

  auto table_schema = table_info_->schema_;
  auto output_schema = plan_->OutputSchema();
  is_same_schema_ = SchemaEqual(&table_schema, output_schema);

  auto transaction = exec_ctx_->GetTransaction();
  auto isolation_level = transaction->GetIsolationLevel();
  auto lock_manager = exec_ctx_->GetLockManager();
  if (isolation_level == IsolationLevel::REPEATABLE_READ) {
    auto iter = table_info_->table_->Begin(transaction);
    while (iter != table_info_->table_->End()) {
      lock_manager->LockShared(transaction, iter->GetRid());
      ++iter;
    }
  }
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto output_schema = plan_->OutputSchema();
  auto table_schema = table_info_->schema_;
  auto predicate = plan_->GetPredicate();
  auto transaction = exec_ctx_->GetTransaction();
  auto lock_manager = exec_ctx_->GetLockManager();
  bool res;

  while (table_iter_ != table_info_->table_->End()) {
    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lock_manager->LockShared(transaction, table_iter_->GetRid());
    }

    res = true;
    auto p_tuple = &(*table_iter_);
    if (predicate != nullptr) {
      res = predicate->Evaluate(p_tuple, &table_schema).GetAs<bool>();
    }

    if (res) {
      if (!is_same_schema_) {
        TupleSchemaTranformUseEvaluate(p_tuple, &table_schema, tuple, output_schema);
      } else {
        *tuple = *p_tuple;
      }
      *rid = p_tuple->GetRid();
      // ++table_iter_;
      // return true;
    }

    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lock_manager->Unlock(transaction, table_iter_->GetRid());
    }
    ++table_iter_;
    if (res) {
      return true;
    }
  }

  return false;
}
}  // namespace bustub
