#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>
#include <mutex>
#include <unordered_set>
#include <unordered_map> //set不知道怎么好用反正map一定好用
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

private:
  // 用链表+哈希表实现
  size_t max_capacity;
  std::list<frame_id_t>lru_list;
  std::unordered_map<frame_id_t,std::list<frame_id_t>::iterator>lru_map;
  std::recursive_mutex latch_;//遇事不决先上锁了
};

#endif  // MINISQL_LRU_REPLACER_H
