#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * Zat Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  return 12 + table_meta_pages_.size() * 8 + index_meta_pages_.size() * 8;
}

CatalogMeta::CatalogMeta() {}

/**
 * Zat Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  //  ASSERT(false, "Not Implemented yet");
    if(init) {
      page_id_t pid; 
      Page *meta_page = buffer_pool_manager_->NewPage(pid);
      ASSERT(pid == CATALOG_META_PAGE_ID, "catalog_meta should be placed in page 0");
      catalog_meta_ = reinterpret_cast<CatalogMeta *>(meta_page->GetData());
      catalog_meta_->NewInstance();
      next_table_id_ = catalog_meta_->GetNextTableId();
      next_index_id_ = catalog_meta_->GetNextIndexId();
      buffer_pool_manager_->UnpinPage(pid, true);
    }
    else {
      // 从文件中薅数据
      Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
      catalog_meta_   = reinterpret_cast<CatalogMeta *>(meta_page->GetData());
      ASSERT(catalog_meta_ != nullptr, "Not init but can't fetch catalog meta page!");
      next_table_id_ = catalog_meta_->GetNextTableId();
      next_index_id_ = catalog_meta_->GetNextIndexId();
      
      for(auto pair_ : catalog_meta_->table_meta_pages_) {
        auto table_id = pair_.first;
        auto page_id  = pair_.second;
        auto table_page = buffer_pool_manager_->FetchPage(page_id);
        TableMetadata *table_metadata = nullptr;
        TableMetadata::DeserializeFrom(table_page->GetData(),table_metadata);
        buffer_pool_manager_->UnpinPage(page_id,false);
        auto table_name = table_metadata->GetTableName();
        auto first_page_id = table_metadata->GetFirstPageId();
        table_names_[table_name] = table_id;
        TableInfo *table_info = TableInfo::Create();
        TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_,first_page_id,table_metadata->GetSchema(),log_manager_,lock_manager_);
        table_info->Init(table_metadata,table_heap);
        tables_[table_id] = table_info;
      }
      for(auto pair_ : catalog_meta_->index_meta_pages_) {
        auto index_id = pair_.first;
        auto page_id  = pair_.second;        
        auto index_page = buffer_pool_manager_->FetchPage(page_id);
        IndexMetadata *index_metadata = nullptr;
        IndexMetadata::DeserializeFrom(index_page->GetData(),index_metadata);
        buffer_pool_manager_->UnpinPage(page_id,false);
        auto index_name = index_metadata->GetIndexName();
        auto table_id   = index_metadata->GetTableId();
        auto table_name = tables_[table_id]->GetTableName();
        index_names_[table_name][index_name] = index_id;
        IndexInfo *index_info = IndexInfo::Create();
        TableInfo *table_info = tables_[table_id];
        index_info->Init(index_metadata,table_info,buffer_pool_manager_);
        indexes_[index_id] = index_info;
      }

      buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,false);
    }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * Zat Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.find(table_name) != table_names_.end()) return DB_TABLE_ALREADY_EXIST;
  ASSERT(table_info == nullptr, "create table with table info ptr not null!");
  page_id_t table_first_page_id;
  auto table_first_page = buffer_pool_manager_->NewPage(table_first_page_id);
  if(table_first_page == nullptr) return DB_FAILED;

  // 记得 unpin!
  TableHeap *table_heap         = TableHeap::Create(buffer_pool_manager_,table_first_page_id,schema,log_manager_,lock_manager_);
  page_id_t table_id            = next_table_id_.fetch_add(1);
  TableMetadata *table_metadata = TableMetadata::Create(table_id,table_name,table_first_page_id,schema);

  // 存metadata
  page_id_t metadata_page_id;
  auto table_metadata_page = buffer_pool_manager_->NewPage(metadata_page_id);
  if(table_metadata_page == nullptr) {
    //存不下metadata，那first page也扔了？
    buffer_pool_manager_->UnpinPage(table_first_page_id,false);
    buffer_pool_manager_->DeletePage(table_first_page_id);
    return DB_FAILED;
  }
  auto buf = table_metadata_page->GetData();
  table_metadata->SerializeTo(buf);
  buffer_pool_manager_->UnpinPage(metadata_page_id,true);

  table_info = TableInfo::Create();
  table_info->Init(table_metadata,table_heap);

  // 存那几个map
  table_names_[table_name] = table_id;
  tables_[table_id] = table_info;
  catalog_meta_->table_meta_pages_[table_id] = metadata_page_id;

  Page *catalog_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  buffer_pool_manager_->UnpinPage(table_first_page_id,true);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}