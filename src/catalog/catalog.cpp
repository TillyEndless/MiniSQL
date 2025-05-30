#include "catalog/catalog.h"
#include <glog/logging.h>

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
      next_index_id_.store(0);
      next_table_id_.store(0);
      catalog_meta_ = new CatalogMeta();
    }
    else {
      // 从文件中薅数据
      Page* meta_page=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
      catalog_meta_=CatalogMeta::DeserializeFrom(meta_page->GetData());
      buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,false);
      next_index_id_.store(catalog_meta_->GetNextIndexId());
      next_table_id_.store(catalog_meta_->GetNextTableId());     
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
  Schema* own_schema = Schema::DeepCopySchema(schema);
  // 这里是新创建表，需要调用第一种构造函数而非第二种，否则会卡死
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, own_schema, txn, log_manager_, lock_manager_);
  page_id_t table_id = next_table_id_.fetch_add(1);
  TableMetadata *table_metadata = TableMetadata::Create(table_id,table_name,table_first_page_id,own_schema);

  // 存metadata
  page_id_t metadata_page_id;
  auto table_metadata_page = buffer_pool_manager_->NewPage(metadata_page_id);
  if(table_metadata_page == nullptr) {
    //存不下metadata，那first page也扔了？
    buffer_pool_manager_->UnpinPage(table_first_page_id,false);
    buffer_pool_manager_->DeletePage(table_first_page_id);
    return DB_FAILED;
  }
  // table_metadata_page->WLatch();
  auto buf = table_metadata_page->GetData();
  table_metadata->SerializeTo(buf);
  // table_metadata_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(metadata_page_id,true);

  table_info = TableInfo::Create();
  table_info->Init(table_metadata,table_heap);

  // 存那几个map
  table_names_[table_name] = table_id;
  tables_[table_id] = table_info;
  catalog_meta_->table_meta_pages_[table_id] = metadata_page_id;

  buffer_pool_manager_->UnpinPage(table_first_page_id,true);

  return DB_SUCCESS;
}

/**
 * Zat Implement
 * 输入table name，如果有就相应修改table_info，否则DB_TABLE_NOT_EXIST
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto it = table_names_.find(table_name);
  if(it == table_names_.end()) return DB_TABLE_NOT_EXIST;
  table_id_t table_id = it->second;
  ASSERT(tables_.find(table_id) != tables_.end(), "CatalogManager::GetTable : find table_id but not table_info");
  table_info = tables_[table_id];
  return DB_SUCCESS;
}

/**
 * Zat Implement
 * return all table info in this catalog manager
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if(!tables.empty()) LOG(INFO) << "CatalogManager ::GetTables : input tables is not empty, will be clear";
  tables.clear();
  for(auto it : tables_) {
    tables.push_back(it.second);
  }
  return DB_SUCCESS;
}

/**
 * Zat Implement
 * 如果按照table_name找不到表返回 DB_TABLE_NOT_EXIST
 * 如果index已经存在返回DB_INDEX_ALREADY_EXIST
 * 否则修改index_info
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // 检查
  auto table_name_it = table_names_.find(table_name);
  if(table_name_it == table_names_.end()) return DB_TABLE_NOT_EXIST;
  // debug记录：find是const的所以不能直接用 ..[..].find()
  // 如果完全没有的话这里at会报错的
  // ASSERT(index_names_.find(table_name) != index_names_.end(), "table exist but index not exist!");
  if(index_names_.find(table_name) != index_names_.end() && index_names_.at(table_name).find(index_name) != index_names_.at(table_name).end()) return DB_INDEX_ALREADY_EXIST;
  
  auto save_index_info = index_info;

  // 获取table信息
  table_id_t table_id = table_name_it->second;
  ASSERT(tables_.find(table_id) != tables_.end(),"CatalogManager::CreateIndex : find table_id but not table_info");
  TableInfo* table_info = tables_[table_id];

  // 构建key map来初始化metadata
  std::vector<uint32_t> key_map;
  key_map.clear();
  // 从index_keys转成uint32_t的key编号，利用schema实现
  Schema* schema = table_info->GetSchema();
  for(auto idx_name : index_keys) {
    uint32_t idx;
    if(schema->GetColumnIndex(idx_name, idx) == DB_COLUMN_NAME_NOT_EXIST) {
      LOG(ERROR) << "CatalogManager::CreateIndex : can't find key name " << idx_name ;
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.push_back(idx);
  }
  
  // 先开个page
  page_id_t meta_page_id;
  Page* meta_page = buffer_pool_manager_->NewPage(meta_page_id);
  if(meta_page == nullptr) {
    LOG(ERROR) << "CatalogManager::CreateIndex : can't NewPage";
    return DB_FAILED;
  }

  // 这些放后面，就不需要因为DB_FAILED和NewPage失败回滚了
  // indexmetadata序列化相关
  index_id_t index_id = catalog_meta_->GetNextIndexId();
  IndexMetadata* index_meta_data = IndexMetadata::Create(index_id,index_name,table_id,key_map);
  meta_page->WLatch();
  index_meta_data->SerializeTo(meta_page->GetData());
  meta_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(meta_page_id, true);
 
  // 更新CatalogMeta并序列化回去
  catalog_meta_->GetIndexMetaPages()->emplace(index_id,meta_page_id);
  // catalog_meta_->index_meta_pages_[index_id] = meta_page_id;
  
  // 这个顺序尽量减少了pin的时间

  index_info = IndexInfo::Create();
  index_info->Init(index_meta_data,table_info,buffer_pool_manager_);

  // 将堆表现存记录插入 index
  auto it = table_info->GetTableHeap()->Begin(nullptr);
  auto end = table_info->GetTableHeap()->End();
  // iterator说要Latch上防止被改了
  for(;it != end; ++it) {
    Row key_row;
    it->GetKeyFromRow(schema, index_info->GetIndexKeySchema(),key_row);
    RowId rid = it->GetRowId();
    page_id_t pid = rid.GetPageId();

    Page* page = buffer_pool_manager_->FetchPage(pid);
    page->RLatch();

    if(index_info->GetIndex()->InsertEntry(key_row, rid, txn) == DB_FAILED){
      LOG(ERROR) << "CatalogManager::CreateIndex : can't insert entry ";
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(pid,false);
      return DB_FAILED;
    }

    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(pid,false);
  }

  // 构建映射
  index_names_[table_name][index_name] = index_id;
  indexes_[index_id] = index_info;

  return DB_SUCCESS;
}

/**
 * Zat Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  if(index_names_.find(table_name) == index_names_.end()) return DB_TABLE_NOT_EXIST;
  auto it = index_names_.at(table_name).find(index_name);
  if(it == index_names_.at(table_name).end()) return DB_INDEX_NOT_FOUND;
  index_info = indexes_.at(it->second);
  return DB_SUCCESS;
}

/**
 * Zat Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  if(index_names_.find(table_name) == index_names_.end()) {
    LOG(ERROR) << "CatalogManaer::GetTableIndexes : can't find table " << table_name <<"'s index";
    return DB_TABLE_NOT_EXIST;
  }
  if(!indexes.empty()) LOG(INFO) << "CatalogManager::GetTableIndexes : indexes not empty at beginning";
  indexes.clear();
  for(auto it : index_names_.at(table_name)) {
    indexes.push_back(indexes_.at(it.second));
  }
  return DB_SUCCESS;
}

/**
 * Zat Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if(table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  std::vector<IndexInfo*>_indexes;
  _indexes.clear();
  if(GetTableIndexes(table_name,_indexes) != DB_SUCCESS) {
    LOG(ERROR) << "CatalogManager::DropTable : can't get table" << table_name << "'s indexes";
    return DB_FAILED;
  }
  for(auto it : _indexes) {
    if(DropIndex(table_name,it->GetIndexName()) != DB_SUCCESS) {
      LOG(ERROR) << "CatalogManager::DropTable : can't delete index" << it->GetIndexName();
      return DB_FAILED; 
    }
  }
  table_id_t table_id = table_names_[table_name];
  page_id_t table_page_id = catalog_meta_->GetTableMetaPages()->at(table_id);
  if(buffer_pool_manager_->DeletePage(table_page_id) != true)
    LOG(ERROR) << "CatalogManager::DropTable : can't delete page";
  catalog_meta_->GetTableMetaPages()->erase(table_id);
  tables_.erase(table_id);
  table_names_.erase(table_name);

  // 不用管写回磁盘，由FlushCatalogMetaPage搞定
  return DB_SUCCESS;
}

/**
 * Zat Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  if(index_names_.find(table_name) == index_names_.end()) return DB_TABLE_NOT_EXIST;
  auto it = index_names_.at(table_name).find(index_name);
  if(it == index_names_.at(table_name).end()) return DB_INDEX_NOT_FOUND;
  
  index_id_t index_id = it->second;
  IndexInfo* index_info = indexes_[index_id];
  TableInfo* table_info = tables_[table_names_[table_name]];
  
  auto schema = table_info->GetSchema();
  auto key_schema = index_info->GetIndexKeySchema();
  // 之前实现table heap 的时候说要加RLatch [by zat]
  for(auto it = table_info->GetTableHeap()->Begin(nullptr); it != table_info->GetTableHeap()->End(); ++it) {
    Row key_row;
    it->GetKeyFromRow(schema,key_schema,key_row);
    RowId rid = it->GetRowId();
    page_id_t pid = rid.GetPageId();

    Page* page = buffer_pool_manager_->FetchPage(pid);
    // page->RLatch();

    dberr_t flag = index_info->GetIndex()->RemoveEntry(key_row,rid,nullptr);
    
    // page->RUnlatch();
    buffer_pool_manager_->UnpinPage(pid, false);

    if(flag == DB_FAILED) {
      LOG(ERROR) << "CatalogManager::DropIndex : can't delete entry";
      return DB_FAILED;
    }
  }

  catalog_meta_->index_meta_pages_.erase(index_id);
  indexes_.erase(index_id);
  index_names_[table_name].erase(index_name);

  return DB_SUCCESS;
}

/**
 * Zat Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page* meta_page=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if(meta_page==nullptr){
    LOG(ERROR) << "CatalogManager::FlushCatalogMetaPage : can't fetch page 0";
    return DB_FAILED;
  }
  // meta_page->WLatch();
  catalog_meta_->SerializeTo(meta_page->GetData());
  // meta_page->WUnlatch();
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,false);
  return DB_SUCCESS;
}

/**
 * Zat Implement
 * 从page id里读取table_id的数据
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if(tables_.find(table_id) != tables_.end())return DB_TABLE_ALREADY_EXIST;
  
  // 读metadata
  Page* table_page = buffer_pool_manager_->FetchPage(page_id);
  TableMetadata* table_metadata;
  TableMetadata::DeserializeFrom(table_page->GetData(), table_metadata);
  
  TableHeap* table_heap=TableHeap::Create(buffer_pool_manager_,page_id,table_metadata->GetSchema(),log_manager_, lock_manager_);

  TableInfo *table_info=TableInfo::Create();
  table_info->Init(table_metadata,table_heap);

  // map
  catalog_meta_->table_meta_pages_[table_id] = page_id;
  table_names_[table_metadata->GetTableName()] = table_metadata->GetTableId();
  tables_[table_id] = table_info;

  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * Zat Implement
 * 从page_id 读 index_id 的数据
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if(indexes_.find(index_id) != indexes_.end()) return DB_INDEX_ALREADY_EXIST;

  Page *index_page=buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata *index_metadata;
  IndexMetadata::DeserializeFrom(index_page->GetData(), index_metadata);
  
  // index metadata 只存了 table_id ，需要反向找到table_name来建立map
  table_id_t tid = index_metadata->GetTableId();
  std::string table_name=tables_[tid]->GetTableName();
  IndexInfo *index_info = IndexInfo::Create();
  index_info->Init(index_metadata, tables_[tid],buffer_pool_manager_);
  
  catalog_meta_->index_meta_pages_[index_id]=page_id;
  index_names_[table_name][index_metadata->GetIndexName()] = index_metadata->GetIndexId();
  indexes_[index_id]=index_info;

  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * Zat Implement
 * 从table_id读info而非table_name
 * 不知道为啥放这里
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if(tables_.find(table_id) == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_[table_id];
  return DB_SUCCESS;
}