//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  Tuple child_tuple;
  RID child_rid;

  bool child_res;
  while (true) {
    child_res = child_executor_->Next(&child_tuple, &child_rid);
    if (!child_res) {
      break;
    }
    tuples_.insert(child_tuple);
  }
  tuple_iterator_ = tuples_.cbegin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (tuple_iterator_ == tuples_.cend()) {
    return false;
  }
  *tuple = *tuple_iterator_;
  *rid = tuple->GetRid();
  ++tuple_iterator_;
  return true;
}

}  // namespace bustub
