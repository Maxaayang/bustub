//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { capacity_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

// 寻找一个页进行换出
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (lru_umap_.empty()) {
    return false;
  }

  *frame_id = lru_list_.front();
  lru_umap_.erase(*frame_id);
  lru_list_.pop_front();

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (lru_umap_.count(frame_id) != 0) {
    lru_list_.erase(lru_umap_[frame_id]);
    lru_umap_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (lru_umap_.count(frame_id) != 0) {
    return;
  }

  if (Size() >= capacity_) {
    frame_id_t need_del = lru_list_.front();
    lru_list_.pop_front();
    lru_umap_.erase(need_del);
  }

  lru_list_.emplace_back(frame_id);
  // lru_umap_[frame_id] = lru_list_.begin();
  lru_umap_.insert({frame_id, --lru_list_.end()});
}

size_t LRUReplacer::Size() { return lru_list_.size(); }

}  // namespace bustub
