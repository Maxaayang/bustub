//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) { capacity_ = num_pages; }

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (clock_lru_list_.empty()) {
    return false;
  }

  *frame_id = clock_lru_list_.back();
  clock_lru_list_.pop_back();
  clock_lru_umap_.erase(*frame_id);

  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);

  if (clock_lru_umap_.count(frame_id) != 0) {
    clock_lru_list_.erase(clock_lru_umap_[frame_id]);
    clock_lru_umap_.erase(frame_id);
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);

  if (clock_lru_umap_.count(frame_id) != 0) {
    return;
  }

  if (Size() >= capacity_) {
    frame_id_t need_del = clock_lru_list_.front();
    clock_lru_list_.pop_front();
    clock_lru_umap_.erase(need_del);
  }

  clock_lru_list_.push_front(frame_id);
  clock_lru_umap_[frame_id] = clock_lru_list_.begin();
}

size_t ClockReplacer::Size() { return clock_lru_list_.size(); }

}  // namespace bustub
