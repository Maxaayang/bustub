//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  auto table_oid = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto table_schema = table_info_->schema_;
  auto transaction = exec_ctx_->GetTransaction();
  auto isolation_level = transaction->GetIsolationLevel();
  auto lock_manager = exec_ctx_->GetLockManager();

  Tuple delete_tuple;
  RID delete_rid;
  bool res = child_executor_->Next(&delete_tuple, &delete_rid);
  // bool lock_res;
  Tuple delete_key_tuple;

  if (res) {
    if (isolation_level == IsolationLevel::REPEATABLE_READ) {
      lock_manager->LockUpgrade(transaction, *rid);
    } else {
      lock_manager->LockExclusive(transaction, *rid);
    }

    bool delete_res = table_info_->table_->MarkDelete(delete_rid, transaction);
    if (!delete_res) {
      throw Exception("delete failed");
    }

    for (auto info : index_info_) {
      delete_key_tuple = delete_tuple.KeyFromTuple(table_schema, info->key_schema_, info->index_->GetKeyAttrs());
      info->index_->DeleteEntry(delete_key_tuple, delete_rid, transaction);
      transaction->AppendTableWriteRecord(
          {delete_rid, table_info_->oid_, WType::DELETE, delete_key_tuple, info->index_oid_, exec_ctx_->GetCatalog()});
    }
  }

  return res;
}

}  // namespace bustub
