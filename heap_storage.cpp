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

/*
 *Updated with Kevin's code
 */
void HeapFile::db_open(uint flags)
{
  if(!this->closed)
    return;

  //this->db = db.Db();
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
 *
 */
void HeapFile::drop(void)
{

  this->open();

  this->close();

  remove(this->dbfilename.c_str());
}

/**
 *Open physical file
 *
 */
void HeapFile::open(void)
{
  this->db_open();

  //overrides _init_parameter?
  //this->block_size = this->stat.db["re_len"];
}

/**
 *Close the physical file
 *
 *
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
 *Updated with Kevin's code
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
 */
BlockIDs *HeapFile::block_ids()
{
  
  BlockIDs *blocks_ids = new BlockIDs();

  for (BlockID i = 1; i < this->last + 1; i++)
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




//////SLOTTED PAGE
typedef u_int16_t u16;

//Constructor
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        this->num_records = get_header()[0]; 
		this->end_free = get_header()[1];
    }
}
// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt *data) {
	if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}
//Get a record from the block. Return null if it has been deleted.
Dbt *SlottedPage::get(RecordID record_id) {
    u16 size = get_header(record_id)[0];
	u16 loc = get_header(record_id)[1];
    if (loc == 0)
        return nullptr;  // this is just a tombstone, record has been deleted
    return new Dbt(this->address(loc), size);
}
//Replace the record with the given data. returns zero if doesn't fit
int SlottedPage::put(RecordID record_id, const Dbt &data) {
	u16 size = get_header(record_id)[0];
	u16 loc = get_header(record_id)[1];
	u16 new_size = (u16) data.get_size();
		if (new_size > size){
			u16 extra = new_size-size;
			if(!has_room(extra)) {
				return 0;
			}
			slide(loc+new_size, loc+size);
			memcpy(this->address(loc), data.get_data(), new_size);
		}
		else{
			memcpy(this->address(loc), data.get_data(), new_size);
			slide(loc+new_size, loc+size);
		}
	size = get_header(record_id)[0];
	loc = get_header(record_id)[1];
	put_header(record_id,new_size,loc);
	
	return 1;
}
// Mark the given id as deleted by changing its size to zero and its location to 0.
// Compact the rest of the data in the block. But keep the record ids the same for everyone.
int SlottedPage::del(RecordID record_id) {
	u16 size = get_header(record_id)[0];
	u16 loc = get_header(record_id)[1];
	put_header(record_id,0,0);
	slide(loc,loc+size);
	return 1;
}
//Sequence of all non-deleted record ids
RecordIDs SlottedPage::ids(void) {
	vector<RecordID> the_ids;
		for(int i = 1; i < num_records+1; i++) {
			if(get_header(i) != 0) {
				the_ids.push_back(i);
			}
		}
	return the_ids;
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
int SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
	return 1;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block header.
int SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
	return 1;
}
//Calculate if we have room to store a record with given size. The size should include the 4 bytes for the header, too, if this is an add.
bool SlottedPage::has_room(u_int16_t size) {
		u16 avail = this->end_free - (num_records+1)*4;
		return size <= avail;
}
/*
		If start < end, then remove data from offset start up to but not including offset end by sliding data
            that is to the left of start to the right. If start > end, then make room for extra data from end to start
            by sliding data that is to the left of start to the left.
            Also fix up any record headers whose data has slid. Assumes there is enough room if it is a left
            shift (end < start).

		
*/
int SlottedPage::slide(u_int16_t start, u_int16_t end) {
	u16 shift = end -start;
	if (shift == 0) {
		return 0;
	}
			
	//sliding
	
	memcpy(this->address(end_free+1+shift),this->address(end_free+1),((end_free+1)-start));
	//block[(end_free+1+shift)::end] = block[(end_free+1)::start];
	vector<RecordID> more_id = ids();
	for(long unsigned int i = 0; i < more_id.size();i++){
		u16 size = get_header(more_id[i])[0];
		u16 loc = get_header(more_id[i])[1];
		if(loc <= start){ 
			loc += shift;
			put_header(more_id[i],size,loc);
		}
	}
	this->end_free += shift;
	put_header();
	return 1;
}
// Get the size for id [0]
// Get the size for offset [1]
u16* SlottedPage::get_header(RecordID id) {
	vector<u16> numbers;
	numbers.push_back(this->get_n(4*id));
	numbers.push_back(this->get_n(4*id+2));
	return numbers.data();
}
//SlottedPage Unit Test

bool everythingCheckForSlotted() {
	char* p = (char*)malloc(400);
	Dbt * block = new Dbt(p,2);
	Dbt pageblock =	Dbt(p,1);
	SlottedPage page(pageblock,(BlockID)0,true);
	if(page.add(block) > 0) {
		printf("Executed SlottedPage Add Funtion");
	}
	if(page.ids().size() > 0){
		printf("Executed SlottedPage ids function");
	}
	block = page.get(1);
	printf("Executed SlottedPage get function");
	free(p);
	p = (char*) malloc(1000);
	block = new Dbt(p,416);
	if(page.put(1,block[0]) > 0){
		printf("Executed SlottedPage put function");
	}
	if(page.del(1) > 0){
		printf("Executed SlottedPage del function");
	}
	
	free(p);
	
	
	return true;
}
