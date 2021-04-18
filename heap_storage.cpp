#include "heap_storage.h"
#include "storage_engine.h"
#include "db_cxx.h"
#include <stdlib>
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
  table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
  cout << "drop ok" << endl;
  HeapTable table("_test_data_cpp", column_names, column_attributes);
  table.create_if_not_exists();
  cout << "create_if_not_exsts ok" << endl;
  ValueDict row;    row["a"] = Value(12);
  row["b"] = Value("Hello!");
  cout << "try insert" << endl;
  table.insert(&row);
  cout << "insert ok" << endl;
  Handles* handles = table.select();
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

HeapFile::HeapFile(string name):DbFile(name),dbfilename(""),last(0),closed(true),db(_DB_ENV,0){}


/**
 *
 *
 *
 */
void HeapFile::_db_open(unit flags)
{
  //closed: bool
  if(!this->closed)
    return;

  this->db = db.DB();
  this->db.set_re_len(this->db.block_size);
  this->dbfilename = this->name + ".db";
  this->db.open(this->dbfilename, NULL, DB_RECNO, flags);
  this->db.stat = this->db.stat(db.DB_FAST_STAT);
  this->last = this->db.stat["ndata"];
  this.closed = false;
  

}

/**
 *Create physical file
 *
 *
 */
void HeapFile::create(void)
{
  this->_db_open(DB_CREATE | DB_EXCL);

  DbBlock *block = this->get_new(); //first block of the file

  this->put(block);

}

/**
 *Delete the physical file
 *
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
 *
 */
void HeapFile::open(void)
{
  this->_db_open();

  //overrides _init_parameter?
  this->block_size = this->stat.db["re_len"];

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
SlottedPage* HeapFile::get_new(void)
{
  //allocate a new block
  char block[DbBlock::BLOCK_SZ]; //BLOCK_SZ = 4096
  memeset(block, 0, sizeof(block));
  Dbt data(block, sizeof(block));

  int block_id = ++this->last;
  Dbt key(&block_id, sizeof(block_id));

  //write ut an empty block and read it back in so BerkDB is managing memory
  SlottedPage* page = new SlottedPage(data, this->last, true);
  this->db.put(nullptr, &key, &data, 0);
  this->db.get(nullptr, &key, &data, 0);

  return page;

}

/**
 *Get a block from the database file
 *@param given block_id 
 *@return a block
 */
SlottedPage* HeapFile::get(BlockId block_id)
{
  //??need fix??
  return SlottedPage(this->db.get(block_id), block_id, false);

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
BlockIDs * HeapFile::block_ids()
{
  //vector<BlockID>

  vector<BlockID>* blocks_ids;

  for (BlockID i = 1; i<(BlockID)this->last+1; i++)
    {
      blocks_ids->push_back(i);
    }

  return blocks_ids;

}






