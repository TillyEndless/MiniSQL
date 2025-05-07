#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages):max_capacity(num_pages) {
  lru_list.clear();
  lru_map.clear();
}
// zat : 原来这里是=default，保险起见我写个clear,初始化也是
LRUReplacer::~LRUReplacer() {
  lru_list.clear();
  lru_map.clear();  
}

/**
 * Zat Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  if(lru_list.empty()) return false;

  auto victim = lru_list.front();
  lru_map.erase(victim);
  lru_list.pop_front();
  *frame_id = victim;
  return true;
}

/**
 * Zat Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  auto it = lru_map.find(frame_id);
  if(it != lru_map.end()){
    lru_list.erase(it->second);
    lru_map.erase(it);
  }
}

/**
 * Zat Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  auto it = lru_map.find(frame_id);
  // 没空插入
  // 连续两次unpin不应该移动到队尾，只有pin+unpin才算一次有效访问，好微妙的设计 [by zat]
  if(it != lru_map.end() || Size() >= max_capacity) return ; //会锁两次所以换成recursive_mutex，不然会直接卡死 [by zat]

  lru_list.push_back(frame_id);
  auto back = lru_list.end();
  --back;//最后一个元素的迭代器
  lru_map[frame_id] = back;
}

/**
 * Zat Implement
 */
size_t LRUReplacer::Size() {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  return lru_list.size();
}