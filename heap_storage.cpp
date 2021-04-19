#include "heap_storage.h"
#include "storage_engine.h"
#include "db_cxx.h"
#include <stdlib.h>
#include <stdio.h>
#include <utility>
#include <map>
#include <vector>
#include <exception>
#include <cstring>
#include <iostream>

using namespace std;

bool test_heap_storage()
{
  ColumnNames column_names;
  column_names.push_back("a");
  column_names.push_back("b");
  ColumnAttributes column_attributes;
  ColumnAttribute ca(ColumnAttribute::INT);
  column_attributes.push_back(ca);
  ca.set_data_type(ColumnAttribute::TEXT);
  column_attributes.push_back(ca);
  HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
  table1.create();
  cout << "create ok" << endl;
  table1.drop(); // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
  cout << "drop ok" << endl;
  HeapTable table("_test_data_cpp", column_names, column_attributes);
  table.create_if_not_exists();
  cout << "create_if_not_exsts ok" << endl;
  ValueDict row;
  row["a"] = Value(12);
  row["b"] = Value("Hello!");
  cout << "try insert" << endl;
  table.insert(&row);
  cout << "insert ok" << endl;
  Handles *handles = table.select();
  cout << "select ok " << handles->size() << endl;
  ValueDict *result = table.project((*handles)[0]);
  cout << "project ok" << endl;
  Value value = (*result)["a"];
  if (value.n != 12)
    return false;
  value = (*result)["b"];
  if (value.s != "Hello!")
    return false;

  table.drop();

  return true;
}
/* FIXME FIXME FIXME */

/**
 * HeapFile
 */


//HeapFile::HeapFile(string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {}

void HeapFile::db_open(uint flags)
{
  //closed: bool                                                                                             
  if(!this->closed)
    return;

  this->db = db.Db();
  this->db.set_re_len(DbBlock::BLOCK_SZ);
  this->dbfilename = this->name + ".db";
  this->db.open(nullptr, this->dbfilename.c_str(),nullptr, DB_RECNO,flags,0644);

  if(flags == 0)
    {
      DB_BTREE_STAT stat;
      this->db.stat(nullptr,&stat, DB_FAST_STAT);
      this->last = stat.bt_ndata;
    }else{
    this->last = 0;
  }

  this->closed = false;

}


/**
 *Create physical file
 */
void HeapFile::create(void)
{
  this->db_open(DB_CREATE | DB_EXCL);

  DbBlock *block = this->get_new(); //first block of the file

  this->put(block);
}

/**
 *Delete the physical file
 */
void HeapFile::drop(void)
{

  this->open();

  this->close();

  remove(this->dbfilename.c_str());
}

/**
 *Open physical file
 */
void HeapFile::open(void)
{
  this->db_open();

  //overrides _init_parameter?
  this->block_size = this->stat.db["re_len"];
}

/**
 *Close the physical file
 */
void HeapFile::close(void)
{
  this->db.close(0);

  this->closed = true;
}

/**
 *Allocate a new block for the database file
 *create a new empty block and add it to the database file
 *@return: the new blcok to be modified by the client
 */
SlottedPage *HeapFile::get_new(void)
{
  //allocate a new block
  char block[DbBlock::BLOCK_SZ]; //BLOCK_SZ = 4096
  memset(block, 0, sizeof(block));
  Dbt data(block, sizeof(block));

  int block_id = ++this->last;
  Dbt key(&block_id, sizeof(block_id));

  //write ut an empty block and read it back in so BerkDB is managing memory
  SlottedPage *page = new SlottedPage(data, this->last, true);
  this->db.put(nullptr, &key, &data, 0);
  this->db.get(nullptr, &key, &data, 0);

  return page;
}

/**
 *Get a block from the database file
 *@param given block_id 
 *@return a block
 */
SlottedPage *HeapFile::get(BlockID block_id)
{
  //??need fix??                                                                                             
  Dbt key(&block_id, sizeof(block_id));
  Dbt data;
  this->db.get(nullptr, &key, &data, 0);

  return new SlottedPage(data, block_id, false);
  
}

/**
 *Write block back to the database file
 *@param block
 *
 */
void HeapFile::put(DbBlock *block)
{
  //need fix?
  int block_id = block->get_block_id();
  Dbt key(&block_id, sizeof(block_id));

  this->db.put(nullptr, &key, block->get_block(), 0);
}

/**
 *iterate through all the blocks ids in the file
 *
 *
 */
BlockIDs *HeapFile::block_ids()
{
  //vector<BlockID>

  vector<BlockID> *blocks_ids;

  for (BlockID i = 1; i < (BlockID)this->last + 1; i++)
  {
    blocks_ids->push_back(i);
  }

  return blocks_ids;
}

/////////////////////////HEAP TABLE (DB_RELATION) /////////////////////////////
HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
{
  this->table_name = table_name;
  this->column_names = column_names;
  this->column_attributes = column_attributes;
}

void HeapTable::create()
{
  this->file.create();
}

void HeapTable::create_if_not_exists()
{
  try
  {
    this->open();
  }
  catch (DbRelationError const &)
  {
    this->create();
  }
}

void HeapTable::drop()
{
  this->file.drop();
}

void HeapTable::open()
{

  this->file.open();
}

void HeapTable::close()
{
  this->file.close();
}

Handle HeapTable::insert(const ValueDict *row)
{
  this->open();
  return this->append(this->validate(row));
}

virtual void del(const Handle handle)
{
  this.open();
  SlottedPage *page = this.file.get(handle.first);
  page.del(handle.second);
  this.file.put(page);
}

virtual Handles *select()
{
  Handles *output = new Handles();
  this->open();
  BlockIDs *blockIDs = this->file.block_ids();
  for (auto const &block_id : *block_ids)
  {
    SlottedPage *page = this.file.get(it.first);
    RecordIDs *recordIDs = page.ids();
    for (auto const &record_id : *record_ids)
    {
      output->push_back(Handle(it, recID));
    }

    delete record_ids;
    delete block;
  }
  return output;
}

ValueDict *HeapTable::validate(const ValueDict *row)
{
  ValueDict *full_row = new ValueDict();
  uint col_num = 0;
  for (Identifier colName : this->column_names)
  {
    ColumnAttribute colAttr = this->column_attributes[col_num++];
    ValueDict::const_iterator found = row->find(colName);
    Value value = found->second;
    if (colAttr.get_data_type() != ColumnAttribute::DataType::INT && colAttr.get_data_type() != ColumnAttribute::DataType::TEXT)
    {
      throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
    }
    else
    {
      value = row->at(colName);
      full_row->at(colName) = value;
    }
  }
  return full_row;
}

Handle HeapTable::append(const ValueDict *row)
{
  Dbt *data = this->marshal(row);
  SlottedPage *block = this->file.get(this->file.get_last_block_id());
  u_int16_t record_id;
  try
  {
    record_id = block->add(data);
  }
  catch (DbRelationError)
  {
    block = this->file.get_new();
    record_id = block->add(data);
  }
  this->file.put(block);
  unsigned int id = this->file.get_last_block_id();
  // Handle output = new Handle(id, record_id);
  return new Handle(id, record_id);
}

Dbt *HeapTable::marshal(const ValueDict *row)
{
  char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
  uint offset = 0;
  uint col_num = 0;
  for (auto const &column_name : this->column_names)
  {
    ColumnAttribute ca = this->column_attributes[col_num++];
    ValueDict::const_iterator column = row->find(column_name);
    Value value = column->second;
    if (ca.get_data_type() == ColumnAttribute::DataType::INT)
    {
      *(int32_t *)(bytes + offset) = value.n;
      offset += sizeof(int32_t);
    }
    else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
    {
      uint size = value.s.length();
      *(u16 *)(bytes + offset) = size;
      offset += sizeof(u16);
      memcpy(bytes + offset, value.s.c_str(), size);
      offset += size;
    }
    else
    {
      throw DbRelationError("Only know how to marshal INT and TEXT");
    }
  }
  char *right_size_bytes = new char[offset];
  memcpy(right_size_bytes, bytes, offset);
  delete[] bytes;
  Dbt *data = new Dbt(right_size_bytes, offset);
  return data;
}

ValueDict *HeapTable::unmarshal(Dbt *data)
{
  std::map<Identifier, Value> *row = {};
  char *bytes = new char[DbBlock::BLOCK_SZ];
  uint offset = 0;
  uint col_num = 0;
  for (auto const &column_name : this->column_names)
  {
    ColumnAttribute ca = this->column_attributes[col_num++];
    ValueDict::const_iterator column = row->find(column_name);
    Value value = column->second;
    if (ca.get_data_type() == ColumnAttribute::DataType::INT)
    {
      value.n = *(int32_t *)(bytes + offset);
      offset += sizeof(int32_t);
    }
    else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
    {
      u16 size = *(u16 *)(bytes + offset);
      offset += sizeof(u16);
      char buffer[DbBlock::BLOCK_SZ];
      memcpy(buffer, bytes + offset, size);
      buffer[size] = '\0';
      value.s = string(buffer); // assume ascii for now
      offset += size;
    }
    else
    {
      throw DbRelationError("Only know how to marshal INT and TEXT");
    }
    (*row)[column_name] = value;
  }
  delete[] bytes;
  return row;
}

bool testHeapTable_CreaetDrop()
{
  // remove file
  if (remove("_test_create_drop.db") != 0)
  {
    perror("Error deleting file");
    return false;
  }

  ColumnNames colNames = new ColumnNames();
  colNames.push_back("a");
  colNames.push_back("b");
  ColumnAttributes colAttrs = new ColumnAttributes();
  colAttrs.push_back(new ColumnAttribute(ColumnAttribute::DataType::INT));
  colAttrs.push_back(new ColumnAttribute(ColumnAttribute::DataType::TEXT));

  HeapTable table = new HeapTable("_test_create_drop.db", colNames, colAttrs);
  table.create();
  if (FILE *file = fopen(name.c_str(), "r"))
  {
    fclose(file);
  }
  else
  {
    perror("db file does not exist");
    return false;
  }

  table.drop();
  if (FILE *file = fopen(name.c_str(), "r"))
  {
    fclose(file);
    perror("db file exists after drop() when it shouldn't");
    return false;
  }

  return true;
}

bool testHeapTable_data()
{
  // remove file
  if (remove("_test_create_drop.db") != 0)
  {
    perror("Error deleting file");
    return false;
  }

  ColumnNames colNames = new ColumnNames();
  colNames.push_back("a");
  colNames.push_back("b");
  ColumnAttributes colAttrs = new ColumnAttributes();
  colAttrs.push_back(new ColumnAttribute(ColumnAttribute::DataType::INT));
  colAttrs.push_back(new ColumnAttribute(ColumnAttribute::DataType::TEXT));

  HeapTable table = new HeapTable("_test_create_drop.db", colNames, colAttrs);
  table.create_if_not_exists() if (FILE *file = fopen(name.c_str(), "r"))
  {
    fclose(file);
  }
  else
  {
    perror("db file does not exist");
    return false;
  }

  table.close();
  table.open();

  ValueDict row1 = new ValueDict();
  row1["a"] = Value(12);
  row1["b"] = Value("Hello!");

  ValueDict row2 = new ValueDict();
  row2["a"] = Value(-192);
  row2["b"] = Value("Much longer piece of text here" * 100);

  ValueDict row3 = new ValueDict();
  row3["a"] = Value(1000);
  row3["b"] = Value("");

  table.insert(row1);
  table.insert(row2);
  table.insert(row3);

  Handles* output = table.select();

  table.drop();
  if (FILE *file = fopen(name.c_str(), "r"))
  {
    fclose(file);
    perror("db file exists after drop() when it shouldn't");
    return false;
  }

  return true;
}
