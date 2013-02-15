#include <leveldb/db.h>
#include <iostream>
#include <sstream>

#define SIZE 1024 * 1024 * 10

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
    arbitraryData[x] = 'A';

  clock_t startTime = clock();

  leveldb::Slice slicedData(arbitraryData, SIZE);

  std::stringstream str;
  str << "key";
  std::string key = str.str();

  status = db->Put(write_options, key, arbitraryData);
  std::cout << key << std::endl;

  std::cout << "Slice is " <<  slicedData.size() << std::endl;
  std::cout << "Wrote to database, status: " << status.ok() << std::endl;
  
  std::cout << "About to do some iteration yo'" << std::endl;

  leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
  for (it->Seek(key); it->Valid() && it->key().ToString() <= key; it->Next()) {
    leveldb::Slice slice = it->value();

    std::stringstream st;
    st << (char)slice.data()[0];
    st << (char)slice.data()[1];
    st << (char)slice.data()[2];
    st << (char)slice.data()[3];

    std::cout << "The first four bytes are " << st.str() << std::endl;
  }

  std::cout << double( clock() - startTime ) / (double)CLOCKS_PER_SEC<< " seconds." << std::endl;

  delete[] arbitraryData;
  delete db;
  std::cout << "Closed database" << std::endl;
}


