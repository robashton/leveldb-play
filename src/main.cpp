#include <iostream>
#include <sstream>
#include <vector>
#include <iterator>

#include "docdb.h"

#define SIZE 1024 * 1024 * 10

int nextOperation();
int createNewDocument();
int checkLastStatus();
int getDocument();
int getAllNewDocuments();

DocDB* store;

int main() {
  store = new DocDB("play/lol");
  checkLastStatus();
  nextOperation();
  delete store;
}

int nextOperation() {
  std::string operation;

  std::cout << "Specify an operation" << std::endl;
  std::cout << "0: Push a document" << std::endl;
  std::cout << "1: Get a document" << std::endl;
  std::cout << "2: Get all new documents" << std::endl;

  std::cin >> operation;

  if(operation == "0")
    createNewDocument();
  else if(operation == "1")
    getDocument();
  else if(operation == "2")
    getAllNewDocuments();
  else
    nextOperation();
}

int createNewDocument() {
  std::string document;
  std::string id;

  std::cout << "Give me a key for the document" << std::endl;
  std::cin >> id;
  std::cout << "Give me the content for the document" << std::endl;
  std::cin >> document;

  std::cout << "Creating a document with id " << id << std::endl;
  store->Put(id, document);

  checkLastStatus();
}

int getDocument() {
  std::string id;
  std::string document;

  std::cout << "Give me a key for the document" << std::endl;
  std::cin >> id;

  store->Get(id, &document);

  std::cout << "Got a document: " << document << std::endl;

  checkLastStatus();
}

int getAllNewDocuments() {
  std::vector<std::string> keys;
  store->GetNewDocuments(keys);

  std::cout << "Searched for documents, listing keys: " << std::endl;

  for(std::vector<std::string>::iterator it = keys.begin(); it != keys.end() ; ++it) {
    std::cout << "Document: " << *it << std::endl;
  }
  nextOperation();
}

int checkLastStatus() {
  std::cout << "Last status: " << store->LastStatus().ok() << std::endl;
}

int openreadtests() {
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
  delete it;
  delete db;
  std::cout << "Closed database" << std::endl;
}


