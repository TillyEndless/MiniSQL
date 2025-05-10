#include "storage/table_heap.h"

/**
 * Zat Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {

  if(row.GetSerializedSize(schema_) >= TablePage::SIZE_MAX_ROW) return false; 
  page_id_t p = first_page_id_;
  page_id_t last_p = first_page_id_;//记录链表末尾的page id方便新开
  
  bool success;
  while(p != INVALID_PAGE_ID) {
    //这里 Pin ! 记得 Unpin!
    TablePage* page = static_cast<TablePage*>(buffer_pool_manager_->FetchPage(p));
    ASSERT(page!=nullptr,"A fetched page but nullptr!");
    
    page->WLatch();
    success = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
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
  success = new_page_->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
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
 * Zat Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page == nullptr) return false; // 不存在没法改

  Row old_row_;
  page->RLatch();
  bool flag_get = page->GetTuple(&old_row_,schema_,txn,lock_manager_);
  page->RUnlatch();
  ASSERT(flag_get,"failed to get old tuple when update");

  page->WLatch();
  bool success = page->UpdateTuple(row,&old_row_,schema_,txn,lock_manager_,log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(),success);

  if(success) {
    row.SetRowId(rid);
    return true;
  }
  else  {
    //说明原地写回失败，原来那个删掉，new一个插入
    if(!MarkDelete(rid,txn)) return false;// 删不掉直接回去（可能是线程有占用？）
    return InsertTuple(row,txn);
  }
}

/**
 * Zat Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  Page *raw = buffer_pool_manager_->FetchPage(rid.GetPageId());
  assert(raw != nullptr);
  auto page = reinterpret_cast<TablePage *>(raw);

  // Step2: Delete the tuple from the page.  
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();

  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
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
 * Zat Implement 调用前记得page RLatch!
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  const RowId rid = row->GetRowId();
  page_id_t page_id = rid.GetPageId();

  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));

  if (page == nullptr) return false;
  bool ok = page->GetTuple(row, schema_, txn, lock_manager_);
  buffer_pool_manager_->UnpinPage(page_id, false);

  return ok;
}

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
 * Zat Implement 调用前记得page RLatch!
 */
TableIterator TableHeap::Begin(Txn *txn) {
  page_id_t pid = first_page_id_;
  
  while (pid != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(pid));
    RowId first_rid;
    if (page->GetFirstTupleRid(&first_rid)) {
      buffer_pool_manager_->UnpinPage(pid, false);
      
      return TableIterator(this, first_rid, txn);
    }
    
    page_id_t next = page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(pid, false);
    pid = next;
  }
  
  return End();
}

/**
 * Zat Implement
 */
TableIterator TableHeap::End() { return TableIterator(this,INVALID_ROWID,nullptr); }
