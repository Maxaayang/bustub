// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // hash_table_bucket_page.cpp
// //
// // Identification: src/storage/page/hash_table_bucket_page.cpp
// //
// // Copyright (c) 2015-2021, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//
// #include <string>
// // using namespace std;

// #include "common/logger.h"
// #include "common/util/hash_util.h"
// #include "storage/index/generic_key.h"
// #include "storage/index/hash_comparator.h"
// #include "storage/page/hash_table_bucket_page.h"
// #include "storage/table/tmp_tuple.h"

// namespace bustub {

// template <typename KeyType, typename ValueType, typename KeyComparator>
// bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
//   bool success = false;
//   for (uint32_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
//     // TODO(max): 修改了
//     // if (!IsReadable(bucket_idx) && !IsOccupied(bucket_idx)) {
//     //   break;
//     // }
//     if (IsReadable(bucket_idx)) {
//       if (cmp(key, array_[bucket_idx].first) == 0) {
//         result->push_back(array_[bucket_idx].second);
//         success = true;
//       }
//     } else if (!IsOccupied(bucket_idx)) {
//       break;
//     }
//   }
//   return success;
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
//   uint32_t bucket_size = BUCKET_ARRAY_SIZE;
//   uint32_t insert_idx = bucket_size;

//   // TODO(max): 修改了
//   // if (IsFull()) {
//   //   return false;
//   // }

//   for (uint32_t i = 0; i < bucket_size; i++) {
//     if (IsReadable(i) && cmp(key, array_[i].first) == 0 && value == array_[i].second) {
//       return false;
//     }

//     if (!IsReadable(i)) {
//       if (insert_idx == bucket_size) {
//         insert_idx = i;
//       }

//       if (!IsOccupied(i)) {
//         break;
//       }
//     }
//   }

//   if (insert_idx == bucket_size) {
//     return false;
//   }

//   array_[insert_idx].first = key;
//   array_[insert_idx].second = value;
//   SetOccupied(insert_idx);
//   SetReadable(insert_idx);

//   return true;
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
//   uint32_t size = BUCKET_ARRAY_SIZE;
//   for (uint32_t i = 0; i < size; i++) {
//     if (IsReadable(i)) {
//       if (cmp(key, array_[i].first) == 0 && value == array_[i].second) {
//         uint32_t index = i / 8;
//         uint32_t offset = i % 8;
//         (readable_[index] &= (0xff - (1 << offset)));
//         return true;
//       }
//     }

//     if (!IsOccupied(i)) {
//       break;
//     }
//   }
//   return false;
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
//   if (IsReadable(bucket_idx)) {
//     return array_[bucket_idx].first;
//   }
//   return {};
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
//   if (IsReadable(bucket_idx)) {
//     return array_[bucket_idx].second;
//   }
//   return {};
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
//   uint32_t index = bucket_idx / 8;
//   uint32_t offset = bucket_idx % 8;
//   (readable_[index] &= ~(1 << offset));
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
//   uint32_t index = bucket_idx / 8;
//   uint32_t offset = bucket_idx % 8;

//   bool success = false;
//   if (((occupied_[index]) & (1 << offset)) != 0) {
//     success = true;
//   }

//   return success;
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
//   uint32_t index = bucket_idx / 8;
//   uint32_t offset = bucket_idx % 8;
//   (occupied_[index] |= (1 << offset));
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
//   uint32_t index = bucket_idx / 8;
//   uint32_t offset = bucket_idx % 8;

//   bool success = false;
//   if (((readable_[index]) & (1 << offset)) != 0) {
//     success = true;
//   }

//   return success;
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
//   uint32_t index = bucket_idx / 8;
//   uint32_t offset = bucket_idx % 8;
//   (readable_[index] |= (1 << offset));
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// uint32_t HASH_TABLE_BUCKET_TYPE::Size() {
//   return BUCKET_ARRAY_SIZE;
// }

// // TODO(max): 这里为什么之前不能判断？
// template <typename KeyType, typename ValueType, typename KeyComparator>
// bool HASH_TABLE_BUCKET_TYPE::IsFull() {
//   // bool success = false;
//   // if (NumReadable() == BUCKET_ARRAY_SIZE) {
//   //   success = true;
//   // }
//   uint32_t exact_div_size = BUCKET_ARRAY_SIZE / 8;
//   for (uint32_t i = 0; i < exact_div_size; i++) {
//     if (readable_[i] != static_cast<char>(0xff)) {
//       return false;
//     }
//   }

//   uint32_t rest = BUCKET_ARRAY_SIZE - BUCKET_ARRAY_SIZE / 8 * 8;
//   return !(rest != 0 && readable_[(BUCKET_ARRAY_SIZE - 1) / 8] != static_cast<char>((1 << rest) - 1));

//   // return success;
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
//   uint32_t size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
//   uint32_t num = 0;
//   for (uint32_t i = 0; i < size; i++) {
//     auto n = static_cast<int>(readable_[i]);
//     // while (n & -n) {
//     //   ++ num;
//     // }

//     while (n != 0) {
//       n &= n - 1;
//       num++;
//     }
//   }

//   return num;
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
//   // int size = sizeof(readable_) / sizeof(readable_[0]);
//   uint32_t size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;

//   for (uint32_t i = 0; i < size; i++) {
//     // if (readable_[i]) {
//     //   success = false;
//     //   break;
//     // }
//     if (readable_[i] != static_cast<char>(0)) {
//       return false;
//     }
//   }

//   return true;
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
//   uint32_t size = 0;
//   uint32_t taken = 0;
//   uint32_t free = 0;
//   for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
//     if (!IsOccupied(bucket_idx)) {
//       break;
//     }

//     size++;

//     if (IsReadable(bucket_idx)) {
//       taken++;
//     } else {
//       free++;
//     }
//   }

//   LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// std::vector<MappingType> HASH_TABLE_BUCKET_TYPE::GetAllItem() {
//   uint32_t bucket_size = BUCKET_ARRAY_SIZE;
//   std::vector<MappingType> items;
//   items.reserve(bucket_size);
//   for (uint32_t i = 0; i < bucket_size; i++) {
//     if (IsReadable(i)) {
//       items.emplace_back(array_[i]);
//     }
//   }
//   return items;
// }

// // DO NOT REMOVE ANYTHING BELOW THIS LINE
// template class HashTableBucketPage<int, int, IntComparator>;

// template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
// template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
// template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
// template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
// template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// // template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

// }  // namespace bustub
