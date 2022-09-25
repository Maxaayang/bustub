//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

void HashJoinExecutor::TupleSchemaTranformUseEvaluateJoin(const Tuple *left_tuple, const Schema *left_schema,
                                                          const Tuple *right_tuple, const Schema *right_schema,
                                                          Tuple *dest_tuple, const Schema *dest_schema) {
  auto columns = dest_schema->GetColumns();
  std::vector<Value> values;
  values.reserve(columns.size());
  for (const auto &col : columns) {
    values.emplace_back(col.GetExpr()->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema));
  }
  *dest_tuple = Tuple(values, dest_schema);
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  hash_table_.clear();
  Tuple right_tuple;
  RID right_rid;
  Value right_key;
  auto right_schema = right_child_->GetOutputSchema();

  bool right_res;
  while (true) {
    right_res = right_child_->Next(&right_tuple, &right_rid);
    if (!right_res) {
      break;
    }

    right_key = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_schema);
    if (hash_table_.count(right_key) == 0) {
      hash_table_.insert({right_key, std::vector<Tuple>{right_tuple}});
    } else {
      hash_table_[right_key].emplace_back(right_tuple);
    }
  }

  left_need_next_ = true;
  array_index_ = 0;
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  auto left_schema = left_child_->GetOutputSchema();
  auto right_schema = right_child_->GetOutputSchema();
  auto final_schema = plan_->OutputSchema();
  bool left_res;

  if (hash_table_.empty()) {
    return false;
  }

  if (left_need_next_) {
    do {
      left_res = left_child_->Next(&left_tuple_, &left_rid_);
      if (!left_res) {
        return false;
      }
      left_key_ = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple_, left_schema);
    } while (hash_table_.count(left_key_) == 0);
    left_need_next_ = false;
  }

  while (true) {
    if (array_index_ >= hash_table_[left_key_].size()) {
      do {
        left_res = left_child_->Next(&left_tuple_, &left_rid_);
        if (left_res) {
          std::cout << "true" << std::endl;
        }
        if (!left_res) {
          return false;
        }
        left_key_ = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple_, left_schema);
      } while (hash_table_.count(left_key_) == 0);
      array_index_ = 0;
    }

    TupleSchemaTranformUseEvaluateJoin(&left_tuple_, left_schema, &hash_table_[left_key_][array_index_], right_schema,
                                       tuple, final_schema);
    *rid = tuple->GetRid();
    array_index_++;
    return true;
  }
  return false;
}

}  // namespace bustub
