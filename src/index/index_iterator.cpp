#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

/**
 * TODO: Student Implement
 */
std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  return page->GetItem(item_index);
}


/**
 * TODO: Student Implement
 */
IndexIterator &IndexIterator::operator++() {
  item_index++;

  if (item_index >= page->GetSize()) {
    page_id_t next_id = page->GetNextPageId();

    // 释放当前页
    buffer_pool_manager->UnpinPage(current_page_id, false);

    if (next_id == INVALID_PAGE_ID) {
      // 到达末尾，标记为结束
      current_page_id = INVALID_PAGE_ID;
      page = nullptr;
    } else {
      // 加载下一页
      current_page_id = next_id;
      Page *next_page = buffer_pool_manager->FetchPage(current_page_id);
      page = reinterpret_cast<LeafPage *>(next_page->GetData());
      item_index = 0;
    }
  }

  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}