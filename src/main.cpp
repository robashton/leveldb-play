#include <leveldb/db.h>
#include <iostream>

#define SIZE 1024 * 1024 * 50

int main() {
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::WriteOptions write_options;
  write_options.sync = true;
  leveldb::Status status = leveldb::DB::Open(options, "play/testdb", &db);

  std::cout << "Opened database, status: " << status.ok() << std::endl;

  char* arbitraryData = new char[SIZE];
  for(int x = 0; x < SIZE ; x++) 
    arbitraryData[x] = 32;

  leveldb::Slice slicedData(arbitraryData, SIZE);

  status = db->Put(write_options, "key", arbitraryData);

  std::cout << "Slice is " <<  slicedData.size() << std::endl;
  std::cout << "Wrote to database, status: " << status.ok() << std::endl;

  std::string outputValue;
  status = db->Get(leveldb::ReadOptions(), "key", &outputValue);

  leveldb::Slice outputSlice(outputValue);
  std::cout << "Read from database, status: " << status.ok() << " bytes: " << outputSlice.size() << std::endl;

  delete db;
  std::cout << "Closed database" << std::endl;
}


