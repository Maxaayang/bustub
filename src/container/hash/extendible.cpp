// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // extendible_hash_table.cpp
// //
// // Identification: src/container/hash/extendible_hash_table.cpp
// //
// // Copyright (c) 2015-2021, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include <iostream>
// #include <string>
// #include <utility>
// #include <vector>
// #include <set>

// #include "common/exception.h"
// #include "common/logger.h"
// #include "common/rid.h"
// #include "container/hash/extendible_hash_table.h"

// namespace bustub {

// template <typename KeyType, typename ValueType, typename KeyComparator>
// HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
//                                      const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
//     : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
//   //  implement me!
//   auto dir_page = CreateDirectoryPage(&directory_page_id_);  //申请目录页
//   page_id_t bucket_page_id;

//   buffer_pool_manager_->NewPage(&bucket_page_id);  //申请第一个桶页面
//   dir_page->SetBucketPageId(0, bucket_page_id);

//   buffer_pool_manager_->UnpinPage(directory_page_id_, true);
//   buffer_pool_manager_->UnpinPage(bucket_page_id, false);
// }

// /*****************************************************************************
//  * HELPERS
//  *****************************************************************************/
// /**
//  * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
//  * for extendible hashing.
//  *
//  * @param key the key to hash
//  * @return the downcasted 32-bit hash
//  */
// template <typename KeyType, typename ValueType, typename KeyComparator>
// uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
//   return static_cast<uint32_t>(hash_fn_.GetHash(key));
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
//   // table_latch_.RLock();
//   // TODO(max): 这里为什么不需要锁
//   return (Hash(key) & dir_page->GetGlobalDepthMask());
//   // table_latch_.RUnlock();
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
//   // table_latch_.RLock();
//   return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
//   // table_latch_.RUnlock();
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
//   return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
//   return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// HashTableDirectoryPage *HASH_TABLE_TYPE::CreateDirectoryPage(page_id_t *directory_page_id) {
//   return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_)->GetData());
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::CreateBucketPage(page_id_t *bucket_page_id) {
//   return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(bucket_page_id)->GetData());
// }

// /*****************************************************************************
//  * SEARCH
//  *****************************************************************************/
// template <typename KeyType, typename ValueType, typename KeyComparator>
// bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
//   table_latch_.RLock();
//   auto *directory_page = FetchDirectoryPage();
//   auto bucket_page_id = KeyToPageId(key, directory_page);
//   auto *bucket_page = FetchBucketPage(bucket_page_id);

//   reinterpret_cast<Page *>(bucket_page)->RLatch();
//   auto ret = bucket_page->GetValue(key, comparator_, result);
//   reinterpret_cast<Page *>(bucket_page)->RUnlatch();

//   buffer_pool_manager_->UnpinPage(directory_page_id_, false);
//   buffer_pool_manager_->UnpinPage(bucket_page_id, false);
//   table_latch_.RUnlock();
//   return ret;
// }

// /*****************************************************************************
//  * INSERTION
//  *****************************************************************************/
// template <typename KeyType, typename ValueType, typename KeyComparator>
// bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
//   table_latch_.RLock();
//   auto directory_page = FetchDirectoryPage();
//   page_id_t bucket_page_id = KeyToPageId(key, directory_page);
//   auto *bucket_page = FetchBucketPage(bucket_page_id);

//   // if (bucket_page->IsFull()) {
//   //   buffer_pool_manager_->UnpinPage(directory_page_id_, false);
//   //   buffer_pool_manager_->UnpinPage(bucket_page_id, false);
//   //   table_latch_.RUnlock();
//   //   return SplitInsert(transaction, key, value);
//   // }

//   reinterpret_cast<Page *>(bucket_page)->WLatch();
//   bool ret = bucket_page->Insert(key, value, comparator_);
//   reinterpret_cast<Page *>(bucket_page)->WUnlatch();

//   buffer_pool_manager_->UnpinPage(directory_page_id_, false);
//   buffer_pool_manager_->UnpinPage(bucket_page_id, true);
//   table_latch_.RUnlock();

//   // TODO(max): 为什么判断是否满了以及进行分裂必须放在后边？
//   if (!ret && bucket_page->IsFull()) {
//     ret = SplitInsert(transaction, key, value);
//   }

//   return ret;

// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
//   table_latch_.WLock();

//   auto directory_page = FetchDirectoryPage();

//   // 找到要进行分割的页面
//   uint32_t bucket_page_idx = KeyToDirectoryIndex(key, directory_page);
//   page_id_t bucket_page_id_old = KeyToPageId(key, directory_page);
//   auto *bucket_page_old = FetchBucketPage(bucket_page_id_old);
//   uint32_t local_depth = directory_page->GetLocalDepth(bucket_page_idx);

//   // 如果没有满的话，则重新插入
//   if (!bucket_page_old->IsFull()) {
//     // table_latch_.WUnlock();
//     bool res = bucket_page_old->Insert(key, value, comparator_);
//     buffer_pool_manager_->UnpinPage(directory_page_id_, false);
//     buffer_pool_manager_->UnpinPage(bucket_page_id_old, true);
//     table_latch_.WUnlock();
//     return res;
//   }

//   uint32_t old_local_mask = directory_page->GetLocalDepthMask(bucket_page_idx);
//   uint32_t new_local_mask = old_local_mask + old_local_mask + 1;  //新的掩码
//   uint32_t new_local_hash = bucket_page_idx & new_local_mask;

//   page_id_t bucket_page_id_new;
//   auto *bucket_page_new = CreateBucketPage(&bucket_page_id_new);

//   uint32_t dir_size = directory_page->Size();

//   // 增加要分割的页面的深度
//   // directory_page->IncrLocalDepth(bucket_page_idx);
//   for (uint32_t i = 0; i < dir_size; i++) {
//     if ((i & new_local_mask) == new_local_hash) {
//       directory_page->IncrLocalDepth(i);
//     }
//   }

//   // 分割页面
//   if (local_depth < directory_page->GetGlobalDepth()) {
//     for (uint32_t i = 0; i < dir_size; i++) {
//       page_id_t bucket_page_id = directory_page->GetBucketPageId(i);  //现在两个还指向同一个页面，找到空桶
//       if ((bucket_page_id == bucket_page_id_old) && ((i & new_local_mask) != new_local_hash)) {
//         directory_page->IncrLocalDepth(i);
//         directory_page->SetBucketPageId(i, bucket_page_id_new);
//       }
//     }
//   } else {
//     directory_page->IncrGlobalDepth();
//     uint32_t new_dir_size = directory_page->Size();
//     for (uint32_t i = dir_size; i < new_dir_size; i++) {
//       page_id_t bucket_page_id = directory_page->GetBucketPageId(i - dir_size);
//       uint32_t local_depth = directory_page->GetLocalDepth(i - dir_size);
//       if (bucket_page_id == bucket_page_id_old && ((i & new_local_mask) != new_local_hash)) {
//         directory_page->SetBucketPageId(i, bucket_page_id_new);
//         // TODO(max): 这里为什么不可以增加局部深度？
//         // directory_page->SetLocalDepth(i, local_depth);
//         // directory_page->IncrLocalDepth(i);
//       } else {
//         directory_page->SetBucketPageId(i, bucket_page_id);
//         // directory_page->SetLocalDepth(i, local_depth);
//       }
//       directory_page->SetLocalDepth(i, local_depth);
//     }
//   }

//   // 将旧页面度数据重新插入
//   reinterpret_cast<Page *>(bucket_page_new)->WLatch();
//   reinterpret_cast<Page *>(bucket_page_old)->WLatch();
//   auto bucket_size = bucket_page_old->Size();
//   for (uint32_t i = 0; i < bucket_size; i++) {
//     auto key = bucket_page_old->KeyAt(i);
//     int32_t bucket_page_id = KeyToPageId(key, directory_page);
//     if (bucket_page_id == bucket_page_id_new) {
//       auto value = bucket_page_old->ValueAt(i);
//       bucket_page_old->Remove(key, value, comparator_);
//       bucket_page_new->Insert(key, value, comparator_);
//     }
//   }

//   bool success = false;

//   page_id_t bucket_page_id = KeyToPageId(key, directory_page);
//   if (bucket_page_id == bucket_page_id_old) {
//     success = bucket_page_old->Insert(key, value, comparator_);
//   } else {
//     success = bucket_page_new->Insert(key, value, comparator_);
//   }

//   reinterpret_cast<Page *>(bucket_page_new)->WUnlatch();
//   reinterpret_cast<Page *>(bucket_page_old)->WUnlatch();

//   buffer_pool_manager_->UnpinPage(bucket_page_id_old, true);
//   buffer_pool_manager_->UnpinPage(bucket_page_id_new, true);
//   buffer_pool_manager_->UnpinPage(directory_page_id_, true);

//   table_latch_.WUnlock();
//   return success;
// }

// /*****************************************************************************
//  * REMOVE
//  *****************************************************************************/
// template <typename KeyType, typename ValueType, typename KeyComparator>
// bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
//   table_latch_.RLock();
//   auto directory_page = FetchDirectoryPage();
//   page_id_t bucket_page_id = KeyToPageId(key, directory_page);
//   auto *bucket_page = FetchBucketPage(bucket_page_id);

//   reinterpret_cast<Page *>(bucket_page)->WLatch();
//   bool success = bucket_page->Remove(key, value, comparator_);
//   reinterpret_cast<Page *>(bucket_page)->WUnlatch();

//   buffer_pool_manager_->UnpinPage(directory_page_id_, false);
//   buffer_pool_manager_->UnpinPage(bucket_page_id, success);

//   table_latch_.RUnlock();

//   if (success && bucket_page->IsEmpty()) {
//     Merge(transaction, key, value);
//     while (ExtraMerge(transaction, key, value)) {
//     }
//   }

//   return success;
// }

// /*****************************************************************************
//  * MERGE
//  *****************************************************************************/
// template <typename KeyType, typename ValueType, typename KeyComparator>
// void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
//   table_latch_.WLock();
//   auto directory_page = FetchDirectoryPage();
//   uint32_t dir_size = directory_page->Size();

//   // 找到需要合并的空页，现在不可以删除，因为可能会合并失败
//   uint32_t bucket_page_idx = KeyToDirectoryIndex(key, directory_page);
//   uint32_t bucket_local_depth = directory_page->GetLocalDepth(bucket_page_idx);
//   uint32_t local_mask = directory_page->GetLocalDepthMask(bucket_page_idx);
//   // TODO
//   // uint32_t new_mask = local_mask >> 1;
//   uint32_t new_mask = local_mask ^ (1 << (bucket_local_depth - 1));
//   page_id_t bucket_page_id = KeyToPageId(key, directory_page);
//   auto *bucket_page = FetchBucketPage(bucket_page_id);

//   reinterpret_cast<Page *>(bucket_page)->RLatch();
//   if (bucket_page->IsEmpty() && bucket_local_depth > 0) {
//     // 寻找对应的可以与其合并的页
//     // id不相同，深度相同，乘以新的掩码之后相同，检查是否可以进行合并
//     uint32_t image_idx = directory_page->GetSplitImageIndex(bucket_page_idx);
//     if (image_idx != DIRECTORY_ARRAY_SIZE) {
//       page_id_t image_id = directory_page->GetBucketPageId(image_idx);
//       uint32_t image_depth = directory_page->GetLocalDepth(image_id);
//       for (uint32_t i = 0; i < dir_size; i++) {
//         // TODO(max): why image_depth == bucket_local_depth ?
//         if (((i & local_mask) == (bucket_page_idx & local_mask)) && (image_depth == bucket_local_depth)) {
//           directory_page->SetBucketPageId(i, image_id);
//         }
//       }
//       reinterpret_cast<Page *>(bucket_page)->RUnlatch();
//       buffer_pool_manager_->UnpinPage(bucket_page_id, false);
//       buffer_pool_manager_->DeletePage(bucket_page_id);

//       for (uint32_t i = 0; i < dir_size; i++) {
//         if ((i & new_mask) == (bucket_page_idx & new_mask)) {
//           directory_page->DecrLocalDepth(i);
//         }
//       }
//     }

//     if (directory_page->CanShrink()) {
//       directory_page->DecrGlobalDepth();
//     }
//   } else {
//     reinterpret_cast<Page *>(bucket_page)->RUnlatch();
//     buffer_pool_manager_->UnpinPage(bucket_page_id, false);
//   }

//   buffer_pool_manager_->UnpinPage(directory_page_id_, true);

//   table_latch_.WUnlock();
// }

// // template <typename KeyType, typename ValueType, typename KeyComparator>
// // void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
// //   table_latch_.WLock();
// //   auto dir_page = FetchDirectoryPage();
// //   uint32_t index = KeyToDirectoryIndex(key, dir_page);
// //   page_id_t bucket_page_id = KeyToPageId(key, dir_page);
// //   uint32_t dir_size = dir_page->Size();
// //   uint32_t local_depth = dir_page->GetLocalDepth(index);
// //   uint32_t local_mask = dir_page->GetLocalDepthMask(index);
// //   uint32_t same_mask = local_mask ^ (1 << (local_depth - 1));  // 合并后的掩码，将最高位的1去掉

// //   auto *bucket_page = FetchBucketPage(bucket_page_id);
// //   bool merge_occur = false;  // 标志是否发生合并

// //   if (local_depth > 0 && bucket_page->IsEmpty()) {  // remove函数加的是读锁，有可能插入新值
// //     page_id_t another_bucket_page_id;
// //     uint32_t another_bucket_idx =
// //         dir_page->GetSplitImageIndex(index);  // 与其对应的桶，如果两者深度一致，则可以合并成一个桶
// //     uint32_t another_local_depth = dir_page->GetLocalDepth(another_bucket_idx);
// //     if (another_local_depth == local_depth) {  // 此时可以进行合并操作
// //       merge_occur = true;
// //       another_bucket_page_id = dir_page->GetBucketPageId(another_bucket_idx);
// //     }
// //     if (merge_occur) {
// //       for (uint32_t i = 0; i < dir_size; i++) {
// //         if ((i & local_mask) == (index & local_mask)) {  // 寻找指向空桶的指针,将其指向另一半another_bucket
// //           dir_page->SetBucketPageId(i, another_bucket_page_id);
// //         }
// //       }
// //       buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);  // 先unpin再删除
// //       buffer_pool_manager_->DeletePage(bucket_page_id, nullptr);
// //       for (uint32_t i = 0; i < dir_size; i++) {
// //         if ((i & same_mask) == (index & same_mask)) {  // 将所有指向another_bucket的local depth都减一
// //           dir_page->DecrLocalDepth(i);
// //         }
// //       }

// //       bool ret = dir_page->CanShrink();
// //       if (ret) {  // 降低全局深度
// //         dir_page->DecrGlobalDepth();
// //       }
// //     }
// //   }
// //   if (!merge_occur) {  // 合并未发生
// //     buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
// //   }
// //   buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
// //   table_latch_.WUnlock();
// // }

// // template <typename KeyType, typename ValueType, typename KeyComparator>
// // bool HASH_TABLE_TYPE::ExtraMerge(Transaction *transaction, const KeyType &key, const ValueType &value) {
// //   table_latch_.WLock();
// //   auto dir_page = FetchDirectoryPage();
// //   uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
// //   page_id_t bucket_id = KeyToPageId(key, dir_page);
// //   uint32_t bucket_local_depth = dir_page->GetLocalDepth(bucket_idx);
// //   uint32_t dir_size = dir_page->Size();
// //   uint32_t bucket_mask = dir_page->GetLocalDepthMask(bucket_idx);
// //   uint32_t new_bucket_mask = bucket_mask >> 1;

// //   bool extra_merge_occur = false;
// //   if (bucket_local_depth > 0) {
// //     // uint32_t image_idx = dir_page->GetSplitImageIndex(bucket_idx);
// //     // page_id_t image_id = dir_page->GetBucketPageId(image_idx);
// //     // uint32_t image_local_depth = dir_page->GetLocalDepth(image_idx);
// //     // auto *image_page = FetchBucketPage(image_id);
// //     // reinterpret_cast<Page *>(image_page)->RLatch();
// //     // if (image_idx != DIRECTORY_ARRAY_SIZE && image_page->IsEmpty()) {
// //     //   extra_merge_occur = true;
// //     //   for (uint32_t i = 0; i < dir_size; i++) {
// //     //     page_id_t tmp_bucket_id = dir_page->GetBucketPageId(i);
// //     //     if (tmp_bucket_id == image_id) {
// //     //       dir_page->SetBucketPageId(i, bucket_id);
// //     //       dir_page->DecrLocalDepth(i);
// //     //     } else if (tmp_bucket_id == bucket_id) {
// //     //       dir_page->DecrLocalDepth(i);
// //     //     }
// //     //   }

// //     std::set<page_id_t> del_page;
// //     for (uint32_t i = 0; i < dir_size; i++) {
// //       if (((i & new_bucket_mask) == (bucket_idx & new_bucket_mask)) && (dir_page->GetLocalDepth(i) ==
// bucket_local_depth)) {
// //         page_id_t image_id = dir_page->GetBucketPageId(i);
// //         auto* image_page = FetchBucketPage(image_id);
// //         if (image_id != bucket_id) {
// //           reinterpret_cast<Page *>(image_page)->RLatch();
// //           if (image_page->IsEmpty()) {
// //             extra_merge_occur = true;
// //             dir_page->SetBucketPageId(i, bucket_id);
// //             reinterpret_cast<Page *>(image_page)->RUnlatch();
// //             buffer_pool_manager_->UnpinPage(image_id, false);
// //             // buffer_pool_manager_->DeletePage(image_id);
// //             del_page.insert(image_id);
// //           } else {
// //             reinterpret_cast<Page *>(image_page)->RUnlatch();
// //             buffer_pool_manager_->UnpinPage(image_id, false);
// //           }
// //         }
// //       }
// //     }

// //     for (auto i : del_page) {
// //       buffer_pool_manager_->DeletePage(i);
// //     }
// //       // }

// //     // reinterpret_cast<Page *>(image_page)->RUnlatch();
// //     // buffer_pool_manager_->UnpinPage(image_id, false);
// //     buffer_pool_manager_->UnpinPage(directory_page_id_, true);
// //     // buffer_pool_manager_->DeletePage(image_id);
// //   } else {
// //     // buffer_pool_manager_->UnpinPage(image_id, false);
// //     buffer_pool_manager_->UnpinPage(directory_page_id_, false);
// //     // reinterpret_cast<Page *>(image_page)->RUnlatch();
// //   }
// //   // }

// //   if (dir_page->CanShrink()) {
// //     dir_page->DecrGlobalDepth();
// //   }

// //   table_latch_.WUnlock();
// //   return extra_merge_occur;
// // }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// bool HASH_TABLE_TYPE::ExtraMerge(Transaction *transaction, const KeyType &key, const ValueType &value) {
//   table_latch_.WLock();
//   auto dir_page = FetchDirectoryPage();
//   page_id_t bucket_page_id = KeyToPageId(key, dir_page);
//   uint32_t index = KeyToDirectoryIndex(key, dir_page);
//   uint32_t local_depth = dir_page->GetLocalDepth(index);
//   uint32_t dir_size = dir_page->Size();
//   bool extra_merge_occur = false;
//   if (local_depth > 0) {
//     auto extra_bucket_idx = dir_page->GetSplitImageIndex(index);  // 计算合并完后对应桶
//     auto extra_local_depth = dir_page->GetLocalDepth(extra_bucket_idx);
//     auto extra_bucket_page_id = dir_page->GetBucketPageId(extra_bucket_idx);
//     auto *extra_bucket = FetchBucketPage(extra_bucket_page_id);
//     if (extra_local_depth == local_depth && extra_bucket->IsEmpty()) {
//       extra_merge_occur = true;
//       page_id_t tmp_bucket_page_id;
//       for (uint32_t i = 0; i < dir_size; i++) {
//         tmp_bucket_page_id = dir_page->GetBucketPageId(i);
//         if (tmp_bucket_page_id == extra_bucket_page_id) {  // 如果是空桶，更改指向并将深度减一
//           dir_page->SetBucketPageId(i, bucket_page_id);
//           dir_page->DecrLocalDepth(i);
//         } else if (tmp_bucket_page_id == bucket_page_id) {  // 原先桶只需将深度减一
//           dir_page->DecrLocalDepth(i);
//         }
//       }
//       buffer_pool_manager_->UnpinPage(extra_bucket_page_id, false, nullptr);  // 先unpin再删除
//       buffer_pool_manager_->DeletePage(extra_bucket_page_id, nullptr);
//       bool ret = dir_page->CanShrink();
//       if (ret) {  // 降低全局深度
//         dir_page->DecrGlobalDepth();
//       }
//     }
//     if (!extra_merge_occur) {  // 额外的合并未发生
//       buffer_pool_manager_->UnpinPage(extra_bucket_page_id, false, nullptr);
//     }
//   }
//   buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
//   buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
//   table_latch_.WUnlock();
//   return extra_merge_occur;
// }

// /*****************************************************************************
//  * GETGLOBALDEPTH - DO NOT TOUCH
//  *****************************************************************************/
// template <typename KeyType, typename ValueType, typename KeyComparator>
// uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
//   table_latch_.RLock();
//   HashTableDirectoryPage *dir_page = FetchDirectoryPage();
//   uint32_t global_depth = dir_page->GetGlobalDepth();
//   assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
//   table_latch_.RUnlock();
//   return global_depth;
// }

// /*****************************************************************************
//  * VERIFY INTEGRITY - DO NOT TOUCH
//  *****************************************************************************/
// template <typename KeyType, typename ValueType, typename KeyComparator>
// void HASH_TABLE_TYPE::VerifyIntegrity() {
//   table_latch_.RLock();
//   HashTableDirectoryPage *dir_page = FetchDirectoryPage();
//   dir_page->VerifyIntegrity();
//   assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
//   table_latch_.RUnlock();
// }

// /*****************************************************************************
//  * TEMPLATE DEFINITIONS - DO NOT TOUCH
//  *****************************************************************************/
// template class ExtendibleHashTable<int, int, IntComparator>;

// template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
// template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
// template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
// template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
// template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

// }  // namespace bustub
