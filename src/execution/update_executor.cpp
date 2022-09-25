//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  auto table_oid = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}


// 这里为什么不使用函数自己的 tuple ？
bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto table_schema = table_info_->schema_;
  auto transaction = exec_ctx_->GetTransaction();
  auto lock_manager = exec_ctx_->GetLockManager();
  Tuple update_tuple;
  Tuple child_tuple;
  RID update_rid;
  RID child_rid;
  bool res = child_executor_->Next(&child_tuple, &child_rid);

  if (res) {
    update_tuple = GenerateUpdatedTuple(child_tuple);
    update_rid = update_tuple.GetRid();
    if (transaction->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      lock_manager->LockUpgrade(transaction, child_rid);
    } else {
      lock_manager->LockExclusive(transaction, child_rid);
    }
    // TODO(max): update第二个参数
    table_info_->table_->UpdateTuple(update_tuple, child_rid, transaction);

    Tuple old_key_tuple;
    Tuple new_key_tuple;

    // 将旧的和新的索引都进行重新插入
    for (auto info : index_info_) {
      old_key_tuple = child_tuple.KeyFromTuple(table_schema, info->key_schema_, info->index_->GetKeyAttrs());
      new_key_tuple = update_tuple.KeyFromTuple(table_schema, info->key_schema_, info->index_->GetKeyAttrs());
      info->index_->DeleteEntry(old_key_tuple, child_rid, transaction);
      // 第三个参数为什么不是 update_rid ?
      info->index_->InsertEntry(new_key_tuple, child_rid, transaction);
      // transaction->AppendTableWriteRecord(IndexWriteRecord{update_rid, table_info_->oid_, WType::UPDATE,
      //                                     new_key_tuple, info->index_oid_, exec_ctx_->GetCatalog()});
    }
  }

  return res;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
