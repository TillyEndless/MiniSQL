#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * Zat Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_ >= GetMaxSupportedSize()) return false;
  page_offset = next_free_page_;
  uint32_t bit_index  = page_offset % 8,
           byte_index = page_offset / 8;
  bytes[byte_index] |= (1u << bit_index);
  uint32_t nxt = next_free_page_;
  for( ; nxt < GetMaxSupportedSize() ; ++nxt ) {
    if(IsPageFree(nxt)) break; // find out
  }
  next_free_page_ = nxt;
  page_allocated_ ++;
  return true;
}

/**
 * Zat Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if( IsPageFree(page_offset) ) return false;
  uint32_t bit_index  = page_offset % 8,
           byte_index = page_offset / 8;
  bytes[byte_index] ^= (1u << bit_index);
  page_allocated_ --;
  if(page_offset < next_free_page_) next_free_page_ = page_offset;
  return true;
}

/**
 * Zat Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint32_t bit_index  = page_offset % 8,
           byte_index = page_offset / 8;
  return IsPageFreeLow(byte_index,bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return ( (bytes[byte_index]>>bit_index) & 1 ) ? false : true;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;