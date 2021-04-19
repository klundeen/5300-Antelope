//File Created 4/16
#include "DbBlock.h"
#include "db_cxx.h"
#include <bit>
#include <vector>
#include <arpa/inet.h>
using namespace std;

const int DB_BLOCK_SIZE = 4096
class SlottedPage: public DbBlock {
	
		SlottedPage(){
			id = 0;
			num_records = 0;
			end_free = DB_BLOCK_SIZE-1;
			block = new char[DB_BLOCK_SIZE];
			put_Header();
		}
		SlottedPage(char* new_block, int block_id){
			block = new_block;
			id = block_id;
			num_records = get_Header_size(0);
			end_free = get_Header_offset(0);
		}
		// Add a new record to the block. Return its id.
		int add(Dbt* data){
			if(!has_Room(sizeof(data)+4)) {
				return 0;
			}
			num_records++;
			int id = num_records;
			int size = sizeof(data);
			end_free = end_free-size;
			int loc = end_free+1;
			put_Header();
			put_Header(id,size,loc);
			block[loc::loc+size] = data;
			return id;
		}
		//Get a record from the block. Return None if it has been deleted.
		Dbt* get(int id){
			int size = get_Header_size(id);
			int loc = get_Header_offset(id);
			if (loc == 0)
				return null;
			return block[loc::loc+size];
		}
		// Mark the given id as deleted by changing its size to zero and its location to 0.
        // Compact the rest of the data in the block. But keep the record ids the same for everyone.
		int del(int id){
			int size = get_Header_size(id);
			int loc = get_Header_offset(id);
			put_Header(id,0,0);
			slide(loc,loc+size);
			return 1;
		}
		//Replace the record with the given data. returns zero if doesn't fit
		int put(Dbt* data, id){
			int size = get_Header_size(id);
			int loc = get_Header_offset(id);
			int new_size = sizeof(data);
			if (new_size > size){
				int extra = new_size-size;
				if(!has_Room(sizeof(data)+4)) {
					return 0;
				}
				block[loc::loc + new_size] = data;
				slide(loc+new_block, loc+size);
			}
			int size = get_Header_size(id);
			int loc = get_Header_offset(id);
			put_Header(id,new_size,loc);
			return 1;
		}
		//Sequence of all non-deleted record ids
		int* ids(){
			vector<int> ids;
			for(int i = 0; i < num_records; i++) {
				if(get_Header_offset(i) != 0) {
					ids.push_back(i);
				}
			}
			return ids.data();
		}
		// Get the size for id
		private int get_Header_size(int id) {
			return get_n(4*id);
		}
		// Get the offset for id 
		private int get_Header_offset(int id) {
			return get_n(4*id+2);
		}
		//Put the size and offset for given id. For id of zero, store the block header. 
		private int put_Header(int id = 0, int size = -1, int loc){
			if(size == -1) {
				size = num_records;
				loc = end_free;
			}
			put_n(4*id,size);
			put_n(4*id+2,loc);
			return 1;
		}
		//Calculate if we have room to store a record with given size. The size should include the 4 bytes for the header, too, if this is an add.
		private bool has_Room(int size) {
			avail = end_free - (num_records+1)*4;
			return size <= avail;
		}
		/*
		If start < end, then remove data from offset start up to but not including offset end by sliding data
            that is to the left of start to the right. If start > end, then make room for extra data from end to start
            by sliding data that is to the left of start to the left.
            Also fix up any record headers whose data has slid. Assumes there is enough room if it is a left
            shift (end < start).

		
		*/
		private int slide(int start, int end){
			int shift = end -start;
			if (shift = 0) {
				return 0;
			}
			
			//sliding
			block[end_free+1+shift::end] = block[end_free+1::start];
			
			for(id: ids()){
				int size = get_Header_size(id);
				int loc = get_Header_offset(id);
				if(loc <= start){ 
					loc += shift;
					put_Header(id,size,loc)
				}
			}
			end_free += shift;
			put_Header();
		}
		//Get a 2-byte integer at given offset
		private int get_n(int offset) {
			return buffToInteger(block[offset::offset+2])
		}
		//Put a 2-byte integer at given offset
		private int put_n(int offset, int n) {
			int insert(n, sizeof(n));
			block[offset::offset+2] = htonl(insert); //Big Endian
			return 1;
		}
		//Covernt binary to Int
		private int buffToInteger(char* buffer)
		{
			int a;
			memcpy( &a, buffer, sizeof( int ) );
			return a;
		}
};