//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() {
  child_->Init();
  AggregateKey key;
  AggregateValue value;
  Tuple child_tuple;
  RID child_rid;
  while (true) {
    bool res = child_->Next(&child_tuple, &child_rid);
    if (!res) {
      break;
    }
    key = MakeAggregateKey(&child_tuple);
    value = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
}

void AggregationExecutor::TupleSchemaTranformUseEvaluateAggregate(const std::vector<Value> &group_bys,
                                                                  const std::vector<Value> &aggregates,
                                                                  Tuple *dest_tuple, const Schema *dest_schema) {
  auto columns = dest_schema->GetColumns();
  std::vector<Value> dest_value;
  dest_value.reserve(columns.size());
  for (auto &column : columns) {
    dest_value.emplace_back(column.GetExpr()->EvaluateAggregate(group_bys, aggregates));
  }
  *dest_tuple = Tuple(dest_value, dest_schema);
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  auto output_schema = plan_->OutputSchema();
  auto having_exr = plan_->GetHaving();
  bool having_res;

  while (aht_iterator_ != aht_.End()) {
    having_res = true;
    if (having_exr != nullptr) {
      having_res =
          having_exr->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_).GetAs<bool>();
    }

    if (having_res) {
      TupleSchemaTranformUseEvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_, tuple,
                                              output_schema);
      *rid = tuple->GetRid();
      ++aht_iterator_;
      return true;
    }

    ++aht_iterator_;
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
