#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }

  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * Zat Implement
 */
page_id_t DiskManager::AllocatePage() {
  std::scoped_lock lock(db_io_latch_);
  auto *pm = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  uint32_t offset;

  //==============满了==================
  if(pm->GetAllocatedPages() >= MAX_VALID_PAGE_ID) return INVALID_PAGE_ID;
  
  //=============正常插入=====================
  for (page_id_t extent_index = 0; extent_index < pm->GetExtentNums(); extent_index++) {
    // 每个bitmap取出来看一眼再丢回去，这里for的是逻辑页号
    // meta_page_id就是0
    page_id_t phy_bitmap_addr = META_PAGE_ID + 1 + extent_index * (1 + BITMAP_SIZE);
    char bitmap_buffer[PAGE_SIZE];
    ReadPhysicalPage(phy_bitmap_addr, bitmap_buffer);
    auto *pb = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_buffer);
    if(pb->AllocatePage(offset)){
      // allocate更新的部分写回
      WritePhysicalPage(phy_bitmap_addr, bitmap_buffer);
      // disk manager的meta data更新
      pm->num_allocated_pages_++;
      pm->extent_used_page_[extent_index]++;
      WritePhysicalPage(META_PAGE_ID, meta_data_);
      return extent_index * BITMAP_SIZE + offset;
    }
  }
  
  //==============新开一页===============
  uint32_t new_index = pm->GetExtentNums();
  // meta data 更新 （写回在最后，让所有write集中一点？)
  pm->extent_used_page_[new_index] = 0;
  pm->num_extents_++;
  pm->num_allocated_pages_++;
  pm->extent_used_page_[new_index]++;
  
  // 新开BITMAP_SIZE+1页（位图+数据）（先设为全0）
  page_id_t new_phy_bitmap_addr = META_PAGE_ID + 1 + new_index * (1 + BITMAP_SIZE);
  char zero[PAGE_SIZE] = {0};
  WritePhysicalPage(new_phy_bitmap_addr, zero);
  for (uint32_t i = 1; i <= BITMAP_SIZE; i++) {
    WritePhysicalPage(new_phy_bitmap_addr + i, zero);
  }

  // 空的，offset=0即可
  auto *new_pb = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(zero);
  offset = 0;
  new_pb->AllocatePage(offset);
  WritePhysicalPage(new_phy_bitmap_addr, zero);

  // 更新 meta data
  
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  return new_index * BITMAP_SIZE + offset;
}

/**
 * Zat Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  std::scoped_lock lock(db_io_latch_);
  auto *pm = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

  page_id_t extent_index  = logical_page_id / BITMAP_SIZE;
  page_id_t extent_offset = logical_page_id % BITMAP_SIZE;

  if (extent_index >= pm->GetExtentNums()) return;

  // 访问相应bitmap
  page_id_t phy_bitmap_addr = META_PAGE_ID + 1 + extent_index * (1 + BITMAP_SIZE);
  char bitmap_buffer[PAGE_SIZE];
  ReadPhysicalPage(phy_bitmap_addr, bitmap_buffer);
  auto *pb = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_buffer);

  // 尝试deallocate
  if (pb->DeAllocatePage(extent_offset)) {
    pm->num_allocated_pages_--;
    pm->extent_used_page_[extent_index]--;
    WritePhysicalPage(META_PAGE_ID, meta_data_);
    WritePhysicalPage(phy_bitmap_addr, bitmap_buffer);
  }
}

/**
 * Zat Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  std::scoped_lock lock(db_io_latch_);
  auto *pm = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

  page_id_t extent_index  = logical_page_id / BITMAP_SIZE;
  page_id_t extent_offset = logical_page_id % BITMAP_SIZE;

  if (extent_index >= pm->GetExtentNums()) return false;

  // 访问bitmap
  page_id_t phy_bitmap_addr = META_PAGE_ID + 1 + extent_index * (1 + BITMAP_SIZE);
  char bitmap_buffer[PAGE_SIZE];
  ReadPhysicalPage(phy_bitmap_addr, bitmap_buffer);
  auto *pb = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_buffer);
  return pb->IsPageFree(extent_offset);
}

/**
 * Zat Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  page_id_t Extent_index  = logical_page_id / BITMAP_SIZE;
  page_id_t Extent_offset = logical_page_id % BITMAP_SIZE;
  return 1 + Extent_index * (1 + BITMAP_SIZE) + Extent_offset;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}