//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  is_raw_insert_ = plan_->IsRawInsert();
}

void InsertExecutor::Init() {
  if (!is_raw_insert_) {
    child_executor_->Init();
  } else {
    values_iter_ = plan_->RawValues().cbegin();
  }

  auto table_oid = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto transaction = exec_ctx_->GetTransaction();
  auto table_schema = table_info_->schema_;
  auto lock_manager = exec_ctx_->GetLockManager();
  Tuple insert_tuple;
  RID insert_rid;
  bool res;

  // 寻找可以进行插入的空间
  if (!is_raw_insert_) {
    // TODO(max): 没有加 res
    res = child_executor_->Next(&insert_tuple, &insert_rid);
    insert_rid = insert_tuple.GetRid();
  } else {
    if (values_iter_ == plan_->RawValues().cend()) {
      res = false;
    } else {
      insert_tuple = Tuple(*values_iter_, &table_schema);
      insert_rid = insert_tuple.GetRid();
      ++values_iter_;
      res = true;
    }
  }

  if (res) {
    lock_manager->LockExclusive(transaction, insert_rid);
    table_info_->table_->InsertTuple(insert_tuple, &insert_rid, transaction);
    Tuple new_key_tuple;

    for (auto info : index_info_) {
      new_key_tuple = insert_tuple.KeyFromTuple(table_schema, info->key_schema_, info->index_->GetKeyAttrs());
      info->index_->InsertEntry(new_key_tuple, insert_rid, transaction);
      transaction->AppendTableWriteRecord(IndexWriteRecord{insert_rid, table_info_->oid_, WType::INSERT, new_key_tuple,
                                                           info->index_oid_, exec_ctx_->GetCatalog()});
    }
  }

  return res;
}

}  // namespace bustub
