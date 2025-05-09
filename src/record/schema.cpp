#include "record/schema.h"

/**
 * static constexpr uint32_t SCHEMA_MAGIC_NUM = 200715;
 * std::vector<Column *> columns_;
 * bool is_manage_ = false; 
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t offset = 0;

  MACH_WRITE_UINT32(buf + offset, SCHEMA_MAGIC_NUM);
  offset += sizeof(uint32_t);

  uint32_t col_count = GetColumnCount();
  MACH_WRITE_UINT32(buf + offset, col_count);
  offset += sizeof(uint32_t);

  for(auto col: columns_){
    uint32_t sz = col->SerializeTo(buf + offset);
    offset += sz;
  }

  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t offset = 0;

  offset += sizeof(uint32_t);

  uint32_t col_count = GetColumnCount();
  offset += sizeof(uint32_t);

  for(auto col: columns_){
    uint32_t sz = col->GetSerializedSize();
    offset += sz;
  }

  return offset;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t offset = 0;

  uint32_t magic = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  CHECK(magic == SCHEMA_MAGIC_NUM) << "Schema magic mismatch";
  
  uint32_t col_count = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  
  std::vector<Column *> cols;
  cols.reserve(col_count);
  for (uint32_t i = 0; i < col_count; ++i) {
    Column *col = nullptr;
    uint32_t sz = Column::DeserializeFrom(buf + offset, col);
    offset += sz;
    cols.push_back(col);
  }
  
  schema = new Schema(cols, true);
  return offset;
}