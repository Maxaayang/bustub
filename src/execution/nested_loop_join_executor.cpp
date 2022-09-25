//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_need_next_ = true;
}

void NestedLoopJoinExecutor::TupleSchemaTranformUseEvaluateJoin(const Tuple *left_tuple, const Schema *left_schema,
                                                                const Tuple *right_tuple, const Schema *right_schema,
                                                                Tuple *dest_tuple, const Schema *dest_schema) {
  auto columns = dest_schema->GetColumns();
  std::vector<Value> dest_value;
  dest_value.reserve(columns.size());
  for (auto &column : columns) {
    dest_value.emplace_back(column.GetExpr()->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema));
  }
  *dest_tuple = Tuple(dest_value, dest_schema);
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  auto final_schema = plan_->OutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();
  auto left_schema = left_executor_->GetOutputSchema();
  auto predicate = plan_->Predicate();
  Tuple right_tuple;
  bool right_res;
  bool left_res;
  bool predicate_res;

  if (left_need_next_) {
    left_res = left_executor_->Next(&left_tuple_, &left_rid_);
    right_res = right_executor_->Next(&right_tuple_, &right_rid_);
    left_need_next_ = false;

    if (!left_res || !right_res) {
      return false;
    }

    predicate_res = predicate->EvaluateJoin(&left_tuple_, left_schema, &right_tuple_, right_schema).GetAs<bool>();
    if (predicate_res) {
      TupleSchemaTranformUseEvaluateJoin(&left_tuple_, left_schema, &right_tuple_, right_schema, tuple, final_schema);
      *rid = tuple->GetRid();
      return true;
    }
  }

  while (true) {
    right_res = right_executor_->Next(&right_tuple_, &right_rid_);
    if (!right_res) {
      left_res = left_executor_->Next(&left_tuple_, &left_rid_);
      if (!left_res) {
        return false;
      }
      right_executor_->Init();
      right_executor_->Next(&right_tuple_, &right_rid_);
    }

    predicate_res = predicate->EvaluateJoin(&left_tuple_, left_schema, &right_tuple_, right_schema).GetAs<bool>();
    if (predicate_res) {
      TupleSchemaTranformUseEvaluateJoin(&left_tuple_, left_schema, &right_tuple_, right_schema, tuple, final_schema);
      *rid = tuple->GetRid();
      return true;
    }
  }
}

}  // namespace bustub
