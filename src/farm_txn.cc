// Copyright (c) 2018 The GAM Authors 

#include "structure.h"
#include "farm_txn.h"
#include "chars.h"
#include "log.h"

#include <cstring>

Object::Object(std::string& s, GAddr addr): buf_(s), addr_(addr), pos_(-1) {}

/**
 * @brief write @param size bytes from position @param offset of this object into @param obuf
 *
 * @param obuf
 * @param offset
 * @param size
 *
 * @return number of objects written
 */
osize_t Object::writeTo(char* obuf, int offset, osize_t size) {
  epicAssert(pos_ >= 0);

  if (size == -1) {
    // read all reaming bytes
    size = size_ - offset;
  }

  if (offset + size > size_ || size <= 0 || offset < 0 || size_ == -1)
    // invalid param
    return 0;

  memcpy(obuf, buf_.c_str() + pos_ + offset, size);
  return size;
}

/**
 * @brief replace the object with the content of @param ibuf
 *
 * @param ibuf
 *
 * @return number of bytes within this object
 */
osize_t Object::readFrom(const char* ibuf) {
  pos_ = buf_.size();
  if (this->size_ <= 0)
    return 0;

  buf_.append(ibuf, this->size_);
  return this->size_;
}

/**
 * @brief read @param size bytes from @param ibuf into position @offset of this
 * object
 *
 * @param ibuf
 * @param offset    the position from which this object shall be written 
 * @param size
 *
 * @return  the nubmer of written bytes
 */
osize_t Object::readEmPlace(const char* ibuf, osize_t offset, osize_t size) {
  if (offset < 0 || offset > size_ || size < 0)
    return 0;

  epicAssert(this->size_ >= 0);

  if (this->pos_ == -1) 
    this->pos_ = buf_.length();

  if (offset + size > this->size_) {
    buf_.append(buf_, pos_, offset).append(ibuf, size);
    this->size_ = offset + size;
    this->pos_ = buf_.length() - size_;
  } else {
    buf_.replace(this->pos_ + offset, size, ibuf, size);
  } 

  return size;
}

osize_t Object::serialize(char* dst, osize_t len) {
  if (len < this->getTotalSize())
    return 0;

  char *buf = dst;
  buf += appendInteger((char*)buf, this->version_, this->size_);
  return buf - dst + writeTo(buf, 0, size_);
}

osize_t Object::deserialize(const char* msg, osize_t size) {
  char* buf = (char*)msg;

  int mdsize = sizeof(version_t) + sizeof(osize_t);

  if ((size < 0 && size != -1) || (size >= 0 && size < mdsize))
    return 0;

  // note the addr_ shall not be included in msg
  buf += readInteger(buf, this->version_, this->size_);
  if (size < this->size_ + mdsize && size > 0 )
    return 0;

  epicAssert(!is_version_locked(this->version_));
  //runlock_version(&this->version_);

  readEmPlace(buf, 0, this->size_);
  return this->getTotalSize();
}

const char* Object::toString() {
  static char s[100];
  memset(s, 0, 100);
  sprintf(s, "addr = %lx, version = %lx, size = %d, locked = %d", 
      addr_, version_, size_, is_version_locked(version_)?1:0);
  return s;
}

int TxnContext::generatePrepareMsg(uint16_t wid, char* buf, int len, int& nobj) {
  int pos = 0, cnt = 0;

  for (auto& p : this->write_set_.at(wid)) {
    if (cnt++ < nobj) continue;
    Object* o = p.second.get();
    if (pos + o->getSize() + sizeof(osize_t) + sizeof(GAddr) > len)
      break;
    pos += appendInteger(buf+pos, o->getAddr(), o->getSize());
    //pos += o->serialize(buf+pos);
    pos += o->writeTo(buf+pos);
    nobj++;
  }

  return pos;
}

int TxnContext::generateValidateMsg(uint16_t wid, char* buf, int len, int& nobj ) {
  int pos = 0, cnt = 0;

  for (auto& p: this->read_set_.at(wid)) {
    // skip writable objects
    // if (p.second.use_count() > 1) continue;

    if (cnt++ < nobj) continue;

    Object* o = p.second.get();

    epicAssert(o->getVersion() > 0);

    if (pos + sizeof(version_t) + sizeof(GAddr) > len)
      break;

    pos += appendInteger(buf+pos, o->getAddr(), o->getVersion());
    nobj ++;
  }

  return pos;
}

int TxnContext::generateCommitMsg(uint16_t wid, char* buf, int len) {
  /* this should never be called */
  return 0;
}

Object* TxnContext::getReadableObject(GAddr addr) {
  //if (read_set_[WID(addr)].count(addr) == 0) {
  //    //createReadableObject(addr);
  //    return nullptr;
  //}

  Object* o = nullptr;

  if (read_set_[WID(addr)].count(addr) > 0)
    o = read_set_[WID(addr)][addr].get();
  else if (write_set_[WID(addr)].count(addr) > 0)
    o = write_set_[WID(addr)][addr].get();

  return o;
}

Object* TxnContext::createReadableObject(GAddr addr) {
  epicAssert(read_set_[WID(addr)].count(addr) == 0 && write_set_[WID(addr)].count(addr) == 0 && WID(addr) > 0);

  epicLog(LOG_DEBUG, "Txn %d creates a readable object for address %lx", this->wr_->id, addr);

  this->read_set_[WID(addr)][addr] =
    std::shared_ptr<Object>(new Object(this->buffer_, addr));

  return read_set_[WID(addr)][addr].get();
}

Object* TxnContext::getWritableObject(GAddr addr) {
  //if (write_set_[WID(addr)].count(addr) == 0)
  //    return nullptr;

  return write_set_[WID(addr)].count(addr) > 0 ? write_set_[WID(addr)][addr].get() : nullptr;
}

Object* TxnContext::createWritableObject(GAddr addr) {
  epicAssert(WID(addr) > 0);
  if (write_set_[WID(addr)].count(addr) == 0) {
    if ( read_set_[WID(addr)].count(addr) > 0) {
      /* share the ownership of object */
      this->write_set_[WID(addr)][addr] = this->read_set_[WID(addr)][addr];
    } else {
      this->write_set_[WID(addr)][addr] =
        std::shared_ptr<Object>(new Object(this->buffer_, addr));
    }

    epicLog(LOG_DEBUG, "Txn %d creates a writable object for address %lx", this->wr_->id, addr);
  }
  return write_set_[WID(addr)][addr].get();
}

void TxnContext::getWidForRobj(std::vector<uint16_t>& wid) {
  for (auto& p: this->read_set_) {
    if (getNumRobjForWid(p.first) > 0)
      wid.push_back(p.first);
  }
}

void TxnContext::getWidForWobj(std::vector<uint16_t>& wid) {
  for (auto& p: this->write_set_) {
    if (getNumWobjForWid(p.first) > 0)
      wid.push_back(p.first);
  }
}

void TxnContext::reset() {
  for (auto& p : this->read_set_) {
    p.second.clear();
  }

  for (auto& p : this->write_set_) {
    p.second.clear();
  }

  this->write_set_.clear();
  this->read_set_.clear();
  this->buffer_.clear();
  this->wr_->tx = this;
}
