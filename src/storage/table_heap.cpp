#include "storage/table_heap.h"

/**
 * Zat Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {

  if(row.GetSerializedSize(schema_) >= TablePage::SIZE_MAX_ROW) return false; 
  page_id_t p = first_page_id_;
  page_id_t last_p = first_page_id_;//记录链表末尾的page id方便新开

  while(p != INVALID_PAGE_ID) {
    //这里 Pin ! 记得 Unpin!
    TablePage* page = static_cast<TablePage*>(buffer_pool_manager_->FetchPage(p));
    ASSERT(page!=nullptr,"A fetched page but nullptr!");
    
    page->WLatch();
    bool success = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    
    if(success){
      // 里面修改了 rid
      buffer_pool_manager_->UnpinPage(p, true);
      return true;
    }

    last_p = p;
    page_id_t next_ = page->GetNextPageId();

    buffer_pool_manager_->UnpinPage(p, false);
    p = next_;
  }
  // 没有地方放得下，要新开
  page_id_t new_page_id;
  auto new_page_ = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
  // 这里也 pin 了，记得 unpin
  if(new_page_ == nullptr) return false;

  new_page_->Init(new_page_id, last_p, log_manager_, txn);
  if(last_p != INVALID_PAGE_ID) {
    auto last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_p));
    last_page->WLatch();
    last_page->SetNextPageId(new_page_id);
    last_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(last_p, true);
  }
  else {
    // 这是第一页
    first_page_id_ = new_page_id;
  }

  new_page_->WLatch();
  bool success = new_page_->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  new_page_->WUnlatch();
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return success;  
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) { return false; }

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) { return false; }

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) { return TableIterator(nullptr, RowId(), nullptr); }

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { return TableIterator(nullptr, RowId(), nullptr); }
