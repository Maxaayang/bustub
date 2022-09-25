//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <iostream>
#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  auto is_shared = txn->IsSharedLocked(rid);
  auto is_exc = txn->IsExclusiveLocked(rid);
  auto isolation_level = txn->GetIsolationLevel();
  auto exec_lock_set = txn->GetExclusiveLockSet();
  txn_id_t txn_id = txn->GetTransactionId();

  if (is_shared || is_exc) {
    return true;
  }

  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  std::unique_lock<std::mutex> lock(latch_);
  LockRequest req(txn_id, LockMode::SHARED, txn);
  if (lock_table_.count(rid) == 0) {
    req.granted_ = true;
    lock_table_[rid].request_queue_.emplace(req);
    lock_table_[rid].status_ = RidStatus::SHARED;
  } else {
    // 将比其年轻的写锁请求杀死，因为写锁的事务可能是在这个锁事务中间开始的, 在发生死锁之前进行预防
    // request_location 不是左值，也不可修改
    // map 返回的第一个值是指向新插入的值的位置，第二个是一个pair
    auto request_location = lock_table_[rid].request_queue_.emplace(req).first;
    auto iter = lock_table_[rid].request_queue_.begin();
    auto tmp = iter;
    while (iter != lock_table_[rid].request_queue_.end()) {
      tmp++;

      // 将年轻的写事务中止
      if (iter->txn_id_ > txn->GetTransactionId() && iter->lock_mode_ == LockMode::EXCLUSIVE) {
        iter->tran_->SetState(TransactionState::ABORTED);
      }

      // 将被终止的事务删除掉
      if (iter->tran_->GetState() == TransactionState::ABORTED) {
        if (iter->granted_) {
          lock.unlock();
          Unlock(iter->tran_, rid);
          lock.lock();
        } else {
          lock_table_[rid].request_queue_.erase(iter);
        }
      }

      iter = tmp;
    }

    lock_table_[rid].cv_.notify_all();

    // 如果还没有获得锁的话，可能就是因为有比他老的写事务在持有写锁
    if (!request_location->granted_) {
      if (lock_table_[rid].status_ == RidStatus::SHARED) {
        // 检查是否有较老的写事务
        bool exec_state = false;
        if (lock_table_[rid].upgrading_ != INVALID_TXN_ID && txn_id > lock_table_[rid].upgrading_) {
          exec_state = true;
        } else {
          for (auto iter = lock_table_[rid].request_queue_.begin(); iter != request_location; iter++) {
            if (iter->lock_mode_ == LockMode::EXCLUSIVE) {
              exec_state = true;
              break;
            }
          }
        }

        // 针对是否有写事务在进行做出不同的操作
        if (exec_state) {
          while (txn->GetState() != TransactionState::ABORTED && !request_location->granted_) {
            lock_table_[rid].cv_.wait(lock);
          }
        } else {
          LockRequest &request = const_cast<LockRequest &>(*request_location);
          request.granted_ = true;
        }
      } else if (lock_table_[rid].status_ == RidStatus::EXCLUSIVE) {
        while (txn->GetState() != TransactionState::ABORTED && !request_location->granted_) {
          lock_table_[rid].cv_.wait(lock);
        }
      }
    }
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  lock_table_[rid].share_req_cnt_++;
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  auto txn_id = txn->GetTransactionId();
  auto txn_state = txn->GetState();
  // auto txn_level = txn->GetIsolationLevel();
  auto is_shared = txn->IsSharedLocked(rid);
  auto is_exc = txn->IsExclusiveLocked(rid);

  if (is_shared) {
    return false;
  }
  if (is_exc) {
    return true;
  }
  if (txn_state != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  std::unique_lock<std::mutex> lock(latch_);
  LockRequest lock_request(txn_id, LockMode::EXCLUSIVE, txn);
  if (lock_table_.count(rid) == 0) {
    lock_request.granted_ = true;
    lock_table_[rid].request_queue_.emplace(lock_request);
    lock_table_[rid].status_ = RidStatus::EXCLUSIVE;
    // return true;
  } else {
    auto request_location = lock_table_[rid].request_queue_.emplace(lock_request).first;
    auto iter = lock_table_[rid].request_queue_.begin();
    auto tmp = iter;

    while (iter != lock_table_[rid].request_queue_.end()) {
      // TODO(max): tmp++
      ++tmp;
      if (iter->txn_id_ > txn_id) {
        iter->tran_->SetState(TransactionState::ABORTED);
      }

      if (iter->tran_->GetState() == TransactionState::ABORTED) {
        if (iter->granted_) {
          lock.unlock();
          Unlock(iter->tran_, rid);
          lock.lock();
        } else {
          lock_table_[rid].request_queue_.erase(iter);
        }
      }
      iter = tmp;
    }

    lock_table_[rid].cv_.notify_all();

    while (txn->GetState() != TransactionState::ABORTED && !(request_location->granted_)) {
      lock_table_[rid].cv_.wait(lock);
    }
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

// bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
//   auto is_shared = txn->IsSharedLocked(rid);  // 防止重复加锁
//   auto is_exc = txn->IsExclusiveLocked(rid);
//   auto transaction_state = txn->GetState();
//   auto txn_id = txn->GetTransactionId();

//   if (is_exc) {  // 防止重复加锁
//     return true;
//   }
//   if (is_shared) {
//     return false;
//   }
//   if (transaction_state != TransactionState::GROWING) {  // 判断当前是否为growing阶段
//     txn->SetState(TransactionState::ABORTED);
//     return false;
//   }

//   std::unique_lock<std::mutex> lock(latch_);
//   LockRequest req(txn_id, LockMode::EXCLUSIVE, txn);
//   if (lock_table_.count(rid) == 0) {  // 当前资源未被占用
//     req.granted_ = true;
//     lock_table_[rid].request_queue_.emplace(req);
//     lock_table_[rid].status_ = RidStatus::EXCLUSIVE;
//   } else {
//     auto request_location = lock_table_[rid].request_queue_.emplace(req).first;
//     auto iter = lock_table_[rid].request_queue_.begin();
//     auto next_iter = iter;
//     while (iter != lock_table_[rid].request_queue_.end()) {  // 杀死所有年轻事务
//       ++next_iter;
//       if (iter->txn_id_ > txn_id) {
//         iter->tran_->SetState(TransactionState::ABORTED);
//       }
//       if (iter->tran_->GetState() == TransactionState::ABORTED) {  // 删除队列中中止事务的请求
//         if (iter->granted_) {
//           lock.unlock();
//           Unlock(iter->tran_, rid);  // 将请求从请求队列移除
//           lock.lock();
//         } else {
//           lock_table_[rid].request_queue_.erase(iter);
//         }
//       }
//       iter = next_iter;
//     }
//     lock_table_[rid].cv_.notify_all();                                                       // 唤醒中止事务请求
//     while (txn->GetState() != TransactionState::ABORTED && !(request_location->granted_)) {  // 事务中止或得到保证
//       lock_table_[rid].cv_.wait(lock);
//     }
//   }
//   if (txn->GetState() == TransactionState::ABORTED) {
//     return false;
//   }
//   txn->GetExclusiveLockSet()->emplace(rid);
//   return true;
// }

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  auto txn_state = txn->GetState();
  auto txn_id = txn->GetTransactionId();
  // auto isolation_level = txn->GetIsolationLevel();
  auto is_shared = txn->IsSharedLocked(rid);
  // auto is_exc = txn->IsExclusiveLocked(rid);

  if (!is_shared) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn_state != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (lock_table_[rid].upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  std::unique_lock<std::mutex> lock(latch_);
  lock_table_[rid].upgrading_ = txn_id;
  auto iter = lock_table_[rid].request_queue_.begin();
  auto tmp = iter;

  // 将年轻的事务中止并进行删除
  while (iter != lock_table_[rid].request_queue_.end()) {
    tmp++;
    if (iter->txn_id_ > txn_id) {
      iter->tran_->SetState(TransactionState::ABORTED);
    }

    if (iter->tran_->GetState() == TransactionState::ABORTED) {
      if (iter->granted_) {
        lock.unlock();
        Unlock(iter->tran_, rid);
        lock.lock();
      } else {
        lock_table_[rid].request_queue_.erase(iter);
      }
    }

    iter = tmp;
  }

  while (lock_table_[rid].share_req_cnt_ != 1 && txn->GetState() != TransactionState::ABORTED) {
    lock_table_[rid].cv_.wait(lock);
  }

  auto request_location = lock_table_[rid].request_queue_.begin();
  LockRequest &request = const_cast<LockRequest &>(*request_location);
  assert(request.txn_id_ == lock_table_[rid].upgrading_);
  request.lock_mode_ = LockMode::EXCLUSIVE;
  lock_table_[rid].share_req_cnt_ = 0;
  lock_table_[rid].status_ = RidStatus::EXCLUSIVE;

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  lock_table_[rid].upgrading_ = INVALID_TXN_ID;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  auto is_shared = txn->IsSharedLocked(rid);
  auto is_exc = txn->IsExclusiveLocked(rid);
  auto txn_state = txn->GetState();
  auto isolation_level = txn->GetIsolationLevel();
  auto txn_id = txn->GetTransactionId();

  // TODO(max):
  if (!is_shared && !is_exc) {
    return false;
  }
  if (isolation_level == IsolationLevel::REPEATABLE_READ && txn_state == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  std::unique_lock<std::mutex> lock(latch_);
  for (auto iter = lock_table_[rid].request_queue_.begin(); iter != lock_table_[rid].request_queue_.end(); iter++) {
    if (iter->txn_id_ == txn_id) {
      lock_table_[rid].request_queue_.erase(iter);
      break;
    }
  }

  auto iter = lock_table_[rid].request_queue_.begin();
  auto next_iter = iter;
  while (iter != lock_table_[rid].request_queue_.end()) {
    ++next_iter;
    if (iter->tran_->GetState() == TransactionState::ABORTED && !iter->granted_) {
      lock_table_[rid].request_queue_.erase(iter);
    }
    iter = next_iter;
  }

  bool need_find_next_req = true;
  bool exist_req = false;
  // LockMode lock_mode;

  if (is_shared) {
    lock_table_[rid].share_req_cnt_--;
    if (lock_table_[rid].share_req_cnt_ != 0) {
      need_find_next_req = false;
    }
  }

  auto next_req_iter = lock_table_[rid].request_queue_.begin();
  for (auto iter = lock_table_[rid].request_queue_.begin(); iter != lock_table_[rid].request_queue_.end(); ++iter) {
    if (iter->tran_->GetState() != TransactionState::ABORTED) {
      exist_req = true;
      next_req_iter = iter;
      // lock_mode = iter->lock_mode_;
      break;
    }
  }

  if (need_find_next_req && exist_req) {
    if (next_req_iter->lock_mode_ == LockMode::SHARED) {
      lock_table_[rid].status_ = RidStatus::SHARED;
      for (auto &iter : lock_table_[rid].request_queue_) {
        LockRequest &request = const_cast<LockRequest &>(iter);
        if (request.lock_mode_ == LockMode::EXCLUSIVE) {
          break;
        }
        request.granted_ = true;
      }
    } else {
      lock_table_[rid].status_ = RidStatus::EXCLUSIVE;
      LockRequest &exc_req = const_cast<LockRequest &>(*next_req_iter);
      exc_req.granted_ = true;
    }
  }

  lock_table_[rid].cv_.notify_all();

  if (need_find_next_req && !exist_req) {
    lock_table_.erase(rid);
  }

  if (is_shared) {
    txn->GetSharedLockSet()->erase(rid);
  }
  if (is_exc) {
    txn->GetExclusiveLockSet()->erase(rid);
  }
  return true;
}

}  // namespace bustub
