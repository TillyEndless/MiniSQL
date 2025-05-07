#ifndef MINISQL_CLOCK_REPLACER_H
#define MINISQL_CLOCK_REPLACER_H

#include <algorithm>
#include <list>
#include <unordered_map>
#include <mutex>
#include <queue>
#include <unordered_set>
#include <vector>
#include <iostream>

#include "buffer/replacer.h"
#include "common/config.h"
#include "glog/logging.h"


using namespace std;

/**
 * CLOCKReplacer implements the clock replacement.
 */
class CLOCKReplacer : public Replacer {
 public:
  /**
   * Create a new CLOCKReplacer.
   * @param num_pages the maximum number of pages the CLOCKReplacer will be required to store
   */
  explicit CLOCKReplacer(std::size_t num_pages);

  /**
   * Destroys the CLOCKReplacer.
   */
  ~CLOCKReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  std::size_t Size() override;

 private:
  std::size_t capacity;
  list<frame_id_t> clock_list;               // replacer中可以被替换的数据页
  unordered_map<frame_id_t, bool> clock_status;  // 数据页的存储状态
  list<frame_id_t>::iterator pointer;// 时钟指针
  recursive_mutex latch_;
};

#endif  // MINISQL_CLOCK_REPLACER_H