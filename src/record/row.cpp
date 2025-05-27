#include "record/row.h"
/**
 *  Row format:
 * -------------------------------------------
 * | Header | Field-1 | ... | Field-N |
 * -------------------------------------------
 *  Header format:
 * --------------------------------------------
 * | Field Nums | Null bitmap |
 * -------------------------------------------
 *
 *  std::vector<Field *> fields_; 
 */
/**
 * Zat Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  // 会不会是因为size开大了一点点
  ASSERT(schema->GetColumnCount() <= fields_.size(), "Fields size do not match schema's column size.");
  
  uint32_t offset = 0, 
           field_count = schema->GetColumnCount(),
           bitmap_bytes_count = (field_count + 7)/8;
  // bitmap_bytes_count 的 +7 是为了上取整

  // column没有magic number，可能由于它不是meta data性质的
  MACH_WRITE_UINT32(buf + offset, field_count);
  offset += sizeof(uint32_t);

  // 现场生成bitmap
  std::vector<uint8_t>null_bitmap(bitmap_bytes_count,0);
  for(uint32_t i = 0; i < field_count; ++i) {
    if(fields_[i]->IsNull())
      null_bitmap[i/8] |= (1<<(i%8));
  }
  // 写没有现成的，抄string的版本
  memcpy(buf + offset, null_bitmap.data(), bitmap_bytes_count);
  offset += bitmap_bytes_count;

  for(uint32_t i = 0; i < field_count; ++i) {
    if(fields_[i]->IsNull()) continue;
    uint32_t sz = fields_[i]->SerializeTo(buf + offset);
    offset += sz;    
  }
  
   uint32_t written_count = MACH_READ_UINT32(buf);
   LOG(INFO) << "Serialized field_count = " << written_count;

  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before deserialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");

  uint32_t offset = 0;
  uint32_t field_count = MACH_READ_UINT32(buf+offset);
  offset += sizeof(uint32_t);

   LOG(INFO) << "Deserialized field_count = " << field_count
           << ", schema columns = " << schema->GetColumnCount();

  ASSERT(field_count == schema->GetColumnCount(), "Schema and data field count mismatch.");

  uint32_t bitmap_bytes_count = (field_count + 7) / 8;
  std::vector<uint8_t>null_bitmap(bitmap_bytes_count);
  memcpy(null_bitmap.data(), buf + offset, bitmap_bytes_count);
  offset += bitmap_bytes_count;

  for(uint32_t i = 0; i < field_count; ++i) {
    bool is_null = (null_bitmap[i/8]>>(i%8))&1;
    Field *field = nullptr;
    uint32_t sz = 0;
    sz = Field::DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(), &field, is_null);
    offset += sz;    
    fields_.push_back(field);
  }

  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  
  uint32_t offset = 0, 
  field_count = schema->GetColumnCount(),
  bitmap_bytes_count = (field_count + 7)/8;
  offset += sizeof(uint32_t);
  offset += bitmap_bytes_count;

  for(uint32_t i = 0; i < field_count; ++i) {
    if(fields_[i]->IsNull()) continue;
    uint32_t sz = fields_[i]->GetSerializedSize();
    offset += sz;    
  }

  return offset;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
