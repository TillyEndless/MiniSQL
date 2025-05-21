#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    // 调用基类方法初始化元信息
      SetPageType(IndexPageType::INTERNAL_PAGE);
      SetPageId(page_id);
      SetParentPageId(parent_id);
      SetKeySize(key_size);
      SetMaxSize(max_size);
      SetSize(0); // 初始化时没有任何键值对
}//page_id_t是啥类型
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
    page_id_t left = 0, right = GetSize() -1;
    page_id_t mid;
    while(left < right){
        mid = (left + right)/2;
        if(int cmp = KM.CompareKeys(KeyAt(mid)), key){
            left = mid +1;
        }else{
            right = mid - 1;
        }
    }
  if(left == right)return left;
  else return INVALID_PAGE_ID;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  page_id_t root_page_id = BufferPoolManager::NewPage();
     // Set the size to 2 (two child pointers, one key)
      SetSize(2);

      // Set first pair: dummy key (ignored), value = old left child
      SetValueAt(0, old_value);

      // Set second pair: key = middle key from split, value = right child
      SetKeyAt(1, new_key);
      SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int index = ValueIndex(old_value);
  if(index == -1) return GetSize(); // old_value不存在，保持不变

  int insert_pos = index + 1;
  int size = GetSize();

  //后面所有pair向后移动一位，腾出插入位置
  for(int i = size; i > insert_pos; --i){
    SetKeyAt(i, KeyAt(i-1));
    SetValueAt(i, ValueAt(i-1));
  }
  SetKeyAt(insert_pos, new_key);
  SetValueAt(insert_pos, new_value);

  IncreaseSize(1); //?写了吗,在b_plus_tree_page里面
  return GetSize();
  //return 0;  ?
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
    int size = GetSize();
    int half = size/2;
    int move_num = size - half;

    void *src = PairPtrAt(half);//第half个pair起始位
    //int bytes = move_num * pair_size;//每个pair的总大小为key+value,pair size上面定义过了
    recipient->CopyNFrom(src, move_num, buffer_pool_manager);//copy data

    SetSize(half);//更新
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
    int start_index = GetSize();
    int bytes = size * pair_size; //pair大小
    memcpy(PairPtrAt(start_index), src, bytes);// 把 src 中的内容整体复制到 data_ 中，从 start_index 开始
    IncreaseSize(size);

    for(int i = 0; i < size; ++i){ // 修改每个被移动子节点的 parent_page_id 为当前
        page_id_t child_pid = *reinterpret_cast<page_id_t *>(reinterpret_cast<char*>(src) + i * page_size + GetKeySize());

        Page *child_page = buffer_pool_manager->FetchPage(child_pid);//获取该子节点页
        if(child_page == nullptr){
            throw std::runtime_error("CopyNForm: cannot fetch child page");
        }

        //修改parent 指针为当前页ID
        auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child_node->SetParentPageId(GetPageId());

        //标记脏页并unpin
        buffer_pool_manager->UnpinPage(child_pid, true);
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
    int size = GetSize();
    if(index >= size || size < 0)return;

    //整体前移
    for(int i = index; i < size -1; ++i){
        SetKeyAt(i, KeyAt(i+1));
        SetValueAt(i, ValueAt(i+1));
    }
    SetSize(size -1);
}


/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
    assert(GetSize() == 1);
    page_id_t only_child = ValueAt(0);
    SetSize(0);
    return only_child;
  //return 0;?
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  int recipient_size = recipient->GetSize();

  // Step 1: 插入 middle_key + 本页第一个指针 到 recipient 的末尾
  // 注意：当前页的第一个 key 是 dummy，不能直接用；但第一个 value 是有效的 child
  recipient->SetKeyAt(recipient_size, middle_key);
  recipient->SetValueAt(recipient_size, ValueAt(0));
  recipient->IncreaseSize(1);

  // Step 2: 将本页 [1, size-1] 的键值对全部复制到 recipient
  int move_num = GetSize() - 1;
  void *src = PairPtrAt(1);  // 从 index 1 开始（跳过 dummy）

  recipient->CopyNFrom(src, move_num, buffer_pool_manager);

  // Step 3: 清空当前页
  SetSize(0);
}


/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  // Step 1: 保存将要移动的 child 页 id（当前页 index 0）
  page_id_t first_value = ValueAt(0);

  // Step 2: recipient 添加：key = middle_key，value = first_value
  recipient->CopyLastFrom(middle_key, first_value, buffer_pool_manager);

  // Step 3: 将当前页的键值对整体左移（从 index 1 开始）
  int size = GetSize();
  for (int i = 0; i < size - 1; ++i) {
    SetKeyAt(i, KeyAt(i + 1));
    SetValueAt(i, ValueAt(i + 1));
  }

  // Step 4: 更新 size
  SetSize(size - 1);
}


/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();

  // 插入新 key 和 value 到末尾
  SetKeyAt(size, key);
  SetValueAt(size, value);
  IncreaseSize(1);

  // 更新被收养子页的 parent_page_id
  Page *child_page = buffer_pool_manager->FetchPage(value);
  assert(child_page != nullptr);
  auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);  // 标记脏页
}


/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  int last_index = size - 1;

  // 获取最后一个 key/value
  GenericKey *last_key = KeyAt(last_index);
  page_id_t last_value = ValueAt(last_index);

  // recipient 的 key/value 要整体向后移动一位，为插入腾位置
  int recipient_size = recipient->GetSize();
  for (int i = recipient_size; i > 0; --i) {
    recipient->SetKeyAt(i, recipient->KeyAt(i - 1));
    recipient->SetValueAt(i, recipient->ValueAt(i - 1));
  }

  // 插入 middle_key 和 last_value 到 recipient 的 index 0
  recipient->SetKeyAt(0, middle_key);
  recipient->SetValueAt(0, last_value);
  recipient->IncreaseSize(1);

  // 更新被移动子页的 parent page_id
  Page *child_page = buffer_pool_manager->FetchPage(last_value);
  assert(child_page != nullptr);
  auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(last_value, true);

  // 从当前页删除最后一个元素
  SetSize(size - 1);
}


/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();

  // 整体后移一位，为 index 0 腾位置（只移动 value，key 在 CopyLastToFrontOf 中给定）
  for (int i = size; i > 0; --i) {
    SetKeyAt(i, KeyAt(i - 1));
    SetValueAt(i, ValueAt(i - 1));
  }

  // 插入 value（key 是由 caller 提供的 middle_key）
  SetValueAt(0, value);
  IncreaseSize(1);

  // 修改 child page 的 parent id
  Page *child_page = buffer_pool_manager->FetchPage(value);
  assert(child_page != nullptr);
  auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
}
