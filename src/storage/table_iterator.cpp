#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn):table_heap_(table_heap),current_rid_(rid),txn_(txn) {
  if(rid == INVALID_ROWID) return ;
  bool ok = table_heap_->GetTuple(&current_row_,txn_);
  ASSERT(ok, "Failed to fetch tuple at table iterator init");
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_  = other.table_heap_;
  txn_         = other.txn_;
  current_row_ = other.current_row_;
  current_rid_ = other.current_rid_;
}

TableIterator::~TableIterator() {
  //好像没有需要这里手动释放的指针，先空着 [by zat]
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return (table_heap_  == itr.table_heap_) &&
         (current_rid_ == itr.current_rid_);
  // 不要求txn，不然和end怎么判相等
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !((table_heap_  == itr.table_heap_) &&
           (current_rid_ == itr.current_rid_));
}

const Row &TableIterator::operator*() {
  return current_row_;
}

Row *TableIterator::operator->() {
  return &current_row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  table_heap_  = itr.table_heap_;
  txn_         = itr.txn_;
  current_row_ = itr.current_row_;
  current_rid_ = itr.current_rid_;  
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  // If already at end or uninitialized, do nothing
  if (current_rid_ == INVALID_ROWID || table_heap_ == nullptr) 
    return *this;

  auto bpm = table_heap_->buffer_pool_manager_;
  page_id_t cur_page_id = current_rid_.GetPageId();
  // Pin，免得查找下一条过程中数据改了
  auto page = reinterpret_cast<TablePage *>(bpm->FetchPage(cur_page_id));
  RowId next_rid;
  
  // 页内下一条
  if (page->GetNextTupleRid(current_rid_, &next_rid)) {
    // Unpin after use
    bpm->UnpinPage(cur_page_id, false);
    current_rid_ = next_rid;
    bool ok = table_heap_->GetTuple(&current_row_, txn_);
    ASSERT(ok, "TableIterator::operator++: GetTuple failed");
    return *this;
  }
  // 找下一页
  page_id_t next_page_id = page->GetNextPageId();
  bpm->UnpinPage(cur_page_id, false);
  while (next_page_id != INVALID_PAGE_ID) {
    auto page2 = reinterpret_cast<TablePage *>(bpm->FetchPage(next_page_id));
    // Fetch时 Pin了
    if (page2->GetFirstTupleRid(&next_rid)) {
      bpm->UnpinPage(next_page_id, false);
      current_rid_ = next_rid;
      bool ok = table_heap_->GetTuple(&current_row_, txn_);
      ASSERT(ok, "TableIterator::operator++: GetTuple failed on new page");
      return *this;
    }
    page_id_t pid = page2->GetNextPageId();
    bpm->UnpinPage(next_page_id, false);
    next_page_id = pid;
  }
  // 找不到，返回end
  current_rid_ = INVALID_ROWID;
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) { 
  TableIterator tmp = *this;
  ++(*this);
  return tmp;  
}
