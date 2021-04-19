// Abstract DbBlock Class

class DbBlock {
	public:
		DbBlock();
		DbBlock(Dbt* data);
		int add(Dbt* data);
		Dbt* get(int id);
		int del(int id);
		int put(Dbt* data, id);
		int* ids();
	protected:
		int id;
		int num_records;
		int end_free;
		char* block;
}