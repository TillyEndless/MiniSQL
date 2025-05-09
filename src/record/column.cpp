#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* Zat Implement
* 参照column类中声明顺序写的
*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t offset = 0;

  MACH_WRITE_UINT32(buf + offset, COLUMN_MAGIC_NUM);
  offset += sizeof(uint32_t);

  uint32_t name_length = static_cast<uint32_t>(name_.length());
  MACH_WRITE_UINT32(buf + offset, name_length);
  offset += sizeof(uint32_t);
  MACH_WRITE_STRING(buf + offset, name_);
  offset += name_length;

  MACH_WRITE_UINT32(buf + offset, static_cast<uint32_t>(type_));
  offset += sizeof(uint32_t);

  MACH_WRITE_UINT32(buf + offset, len_);
  offset += sizeof(uint32_t);

  MACH_WRITE_UINT32(buf + offset, table_ind_);
  offset += sizeof(uint32_t);

  // 没有写bool的宏，照着抄两个
  *reinterpret_cast<bool *>(buf + offset) = nullable_;
  offset += sizeof(bool);

  *reinterpret_cast<bool *>(offset) = unique_;
  offset += sizeof(bool);

  return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  return static_cast<uint32_t>(5*sizeof(uint32_t) + 2*sizeof(bool) + name_.length());
}

/**
 * Zat Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  if (column != nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize." 									 << std::endl;
  }

  uint32_t offset = 0;
  uint32_t magic_number = MACH_READ_UINT32(buf + offset);
  CHECK(magic_number == COLUMN_MAGIC_NUM) << "Column magic mismatch";
  offset += sizeof(uint32_t);

  uint32_t name_length = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  std::string name_(buf + offset, name_length);
  offset += name_length;

  TypeId type_ = static_cast<TypeId>(MACH_READ_UINT32(buf + offset));
  offset += sizeof(uint32_t);

  uint32_t len_ = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);

  uint32_t table_ind_ = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);

  uint32_t nullable_ = *reinterpret_cast<bool *>(buf + offset);
  offset += sizeof(bool);

  uint32_t unique_ = *reinterpret_cast<bool *>(offset);
  offset += sizeof(bool);
  
  if( type_ == TypeId::kTypeChar) 
    column = new Column(name_, type_, len_, table_ind_, nullable_, unique_);
  else 
    column = new Column(name_,type_,table_ind_, nullable_, unique_);
  
  return offset;
}
