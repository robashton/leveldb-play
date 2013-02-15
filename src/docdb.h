#include <leveldb/db.h>
#include <iostream>
#include <sstream>
#include <stdlib.h>

#define DOCPREFIX "/docs/";
#define ETAGPREFIX "/etags/";

class DocDB {
  public:
    DocDB(std::string location) {
      this->options.create_if_missing = true;
      this->writeoptions.sync = true;
      this->lastStatus = leveldb::DB::Open(options,location, &this->db);
    }
    ~DocDB() {
      delete this->db;
    }

    leveldb::Status LastStatus() {
      return this->lastStatus;
    }

    void Get(std::string id, std::string* doc) {
      std::stringstream st;
      st << DOCPREFIX;
      st << id;
      std::string key = st.str();
      std::string documentkey = key + "/data";
      this->lastStatus = this->db->Get(leveldb::ReadOptions(), documentkey, doc);
    }

    void Put(std::string id, std::string doc) {
      std::stringstream st;
      st << DOCPREFIX;
      st << id;
      std::string key = st.str();
      std::string metadatakey = key + "/metadata";
      std::string documentkey = key + "/data";
      std::string lastetag = "0";
      std::string etagkey = "";

      this->lastStatus = this->db->Get(leveldb::ReadOptions(), metadatakey, &lastetag);
      
      if(this->lastStatus.ok()) {
        etagkey = ETAGPREFIX;
        etagkey += lastetag;
        this->db->Delete(leveldb::WriteOptions(), etagkey);
      }

      // Ssssh
      int newEtag = atoi(lastetag.c_str()) + 1;
      std::stringstream newetagbuilder;
      newetagbuilder << newEtag;
      std::string newetag = newetagbuilder.str();
      
      etagkey = ETAGPREFIX;
      etagkey += newetag;

      this->lastStatus = this->db->Put(leveldb::WriteOptions(), documentkey, doc);
      this->lastStatus = this->db->Put(leveldb::WriteOptions(), metadatakey, newetag);
      this->lastStatus = this->db->Put(leveldb::WriteOptions(), etagkey, documentkey);
    }
  private:
    leveldb::DB* db;
    leveldb::Options options;
    leveldb::WriteOptions writeoptions;
    leveldb::Status lastStatus;
    int lastIndexedEtag;
};
