#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}
/**
 * Zat Implement
 * free_list first
 * then replacer
 * return INVALID_FRAME_ID if fail
 */
frame_id_t BufferPoolManager::TryToFindFreePage() {
  frame_id_t frame_ = INVALID_FRAME_ID;
  if(!free_list_.empty()){
    frame_ = free_list_.front();
    free_list_.pop_front();
  }
  // LRU 换一个
  else {
    frame_id_t replace_frame_id;
    if(replacer_->Victim(&replace_frame_id)) //有得换
      frame_ = replace_frame_id;
  }
  return frame_;
}
/**
 * Zat Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  auto it = page_table_.find(page_id);
  if(it != page_table_.end()){
    pages_[it->second].pin_count_++;
    replacer_->Pin(it->second);
    return &pages_[it->second];
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  else {
    frame_id_t frame_ = TryToFindFreePage();

    if(frame_ == INVALID_FRAME_ID) return nullptr;
    // 2.     If R is dirty, write it back to the disk.
    // 3.     Delete R from the page table and insert P.
    if(pages_[frame_].page_id_ != INVALID_PAGE_ID) {
      if(pages_[frame_].IsDirty())
        disk_manager_->WritePage(pages_[frame_].page_id_, pages_[frame_].data_);
      page_table_.erase(pages_[frame_].page_id_);    
    }

    page_table_[page_id] = frame_;
    // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    disk_manager_->ReadPage(page_id,pages_[frame_].data_);
    pages_[frame_].pin_count_ = 1;
    pages_[frame_].page_id_ = page_id;
    pages_[frame_].is_dirty_ = false;

    replacer_->Pin(frame_);
    return &pages_[frame_];
  }
}

/**
 * Zat Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  // 0.   Make sure you call AllocatePage!
  page_id = disk_manager_->AllocatePage();
  
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if(page_id == INVALID_PAGE_ID) return nullptr;

  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  
  frame_id_t frame_ = TryToFindFreePage();

  //没地方放的话要回收这个新开的页
  if(frame_ == INVALID_FRAME_ID) {
    disk_manager_->DeAllocatePage(page_id);
    return nullptr;
  }

  //清除替换页
  if(pages_[frame_].page_id_ != INVALID_PAGE_ID) {
    if(pages_[frame_].IsDirty())
      disk_manager_->WritePage(pages_[frame_].page_id_, pages_[frame_].data_);
    page_table_.erase(pages_[frame_].page_id_);    
  }
  
  
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  pages_[frame_].ResetMemory();
  pages_[frame_].pin_count_ = 1;
  pages_[frame_].is_dirty_ = true; //是否应该这么处理存疑 [by zat]
  pages_[frame_].page_id_ = page_id;
  page_table_[page_id] = frame_;
  replacer_->Pin(frame_);
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return &pages_[frame_];
}

/**
 * Zat Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) return true;
  
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if(pages_[it->second].pin_count_ != 0) return false;
  
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  pages_[it->second].pin_count_ = 0;
  pages_[it->second].page_id_ = INVALID_PAGE_ID;
  pages_[it->second].is_dirty_ = 0;

  replacer_->Pin(it->second); // 防止它进lru_list

  free_list_.push_back(it->second);
  page_table_.erase(it);

  // 0.   Make sure you call DeallocatePage!
  disk_manager_->DeAllocatePage(page_id); // 虽然是0但是应该是后面检查完了确实能删除再de allocate
  return true;
}

/**
 * Zat Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) return false;

  frame_id_t frame_ = it->second;

  if (pages_[frame_].pin_count_ <= 0) return false;

  // 这个参数个人理解为由进程告诉manager进程中是否修改了page
  if (is_dirty) pages_[frame_].is_dirty_ = true;

  pages_[frame_].pin_count_--;
  if (pages_[frame_].pin_count_ == 0) replacer_->Unpin(frame_);

  return true;
}

/**
 * Zat Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  auto it = page_table_.find(page_id);
  if(it == page_table_.end()) return false; // 不知道这里t/f的作用是什么 [by zat]

  //需要写回
  disk_manager_->WritePage(pages_[it->second].page_id_,pages_[it->second].GetData());
  pages_[it->second].is_dirty_ = false;
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  LOG(INFO) << "*** BEGIN CheckAllUnpinned ***";
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      // LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
      LOG(ERROR) << "!!! LEAKED PAGE: page_id=" << pages_[i].page_id_
                 << " pin_count=" << pages_[i].pin_count_;
    }
  }
  LOG(INFO) << "*** END CheckAllUnpinned ***";
  return res;
}