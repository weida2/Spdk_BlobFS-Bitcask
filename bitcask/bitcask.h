#ifndef BITCASK_H
#define BITCASK_H

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <pwd.h>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/list.hpp>
#include <boost/crc.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/time_serialize.hpp>

using namespace std;

#define file_max 4 * 1024 * 1024  // 4MB

const string file_prev = "bitcask_data";
const string cmd_prompt = " >>> bitcask : ";
const string cmd = ">>> ";
const int number = 0;
// data
struct bitcask_data {
    string key;
    int key_size;
    string value;
    int value_size;
    boost::posix_time::ptime timestamp;
    uint32_t crc;
    // 序列化函数
    template <typename Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
        ar &key;
        ar &key_size;
        ar &value;
        ar &value_size;
        ar &timestamp;
        ar &crc;
    }
};


// index 
struct bitcask_index {
    string key;
    string file_id;
    int value_size;
    int value_pos;
    boost::posix_time::ptime timestamp;
    bool value_valid;
    // 序列化函数
    template <typename Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
        ar &key;
        ar &file_id;
        ar &value_size;
        ar &value_pos;
        ar &timestamp;
        ar &value_valid;
    }
    // 默认构造函数赋值value_valid为false
    bitcask_index() {
        value_valid = false;
    }
};

// bitcask
class bitcask {
private:
    unordered_map<string, bitcask_index> hash_index;
    int active_file_cnt_;
    bool start_;
    bool finish_;
    string respone_;
    string filepath;

private:
    void init(string path);
    uint32_t crc32(string value);
    void insert_data(string key, string value);
    void write_data(bitcask_data data);
    void write_index(bitcask_index index);
    bitcask_data read_data(string key);
    void read_datainfo(string key);
    bitcask_index read_index(string key);
    // merge
    void delete_data(string key);
    void update_data(string key, string value);
    void update_index(bitcask_index index, string key);
    void merge();
    void flush();

public:
    bitcask();
    ~bitcask();
    string Get(string key);
    void Put(string key, string value);
    void Open(string path);
    void Close();
    void Show();
};


#endif /* BITCASK_H */