#include <iostream>
#include <fstream>
#include <string>
#include <ctime>
#include <unistd.h>
#include <dirent.h>
#include <pwd.h>
#include "bitcask.h"

using namespace std;

bitcask::bitcask() {
    start_ = false;
    finish_ = false;
    active_file_cnt_ = 0;
    respone_ = "";
    filepath = "";   
}

bitcask::~bitcask() {
    if (start_) {
        merge();
        flush();
    }
}

void bitcask::init(string path) {
    this->filepath = path;
    this->start_ = true;
    long len;
    fstream hint;
    // 以写入和二进制模式打开文件, 且仅追加写
    hint.open(path + "hint.bin", ios::out | ios::binary | ios::app);
    if (!hint) {
        cout << "open hint file failed or maybe not exist!" << endl;
    }
    len = hint.tellg();
    if (len == 0) {
        // 文件新创建且为空
        cout << "create file hint.bin successfully" << endl;
    } else {
        // 文件已存在且包含数据
        // load index to memory to costruct hash_index
        cout << "file hint.bin already exists, load index" << endl;
        bitcask_index read_bin;
        fstream hint;
        hint.open(path + "hint.bin", ios::in | ios::binary);
        if (!hint) {
            cout << "open hint file failed or maybe not exist!" << endl;
        }
        while (hint) {
            boost::archive::binary_iarchive ia(hint, boost::archive::no_header);
            try {
                ia >> read_bin;
            } catch (const exception &e) {
                goto do_load;
            }
            bitcask_index insert;
            insert.key = read_bin.key;
            insert.file_id = read_bin.file_id;
            insert.value_size = read_bin.value_size;
            insert.value_pos = read_bin.value_pos;
            insert.timestamp = read_bin.timestamp;
            insert.value_valid = read_bin.value_valid;
            hash_index[read_bin.key] = insert;
        }
    }
do_load:
    hint.close();
    // load data
    fstream file_log;
    file_log.open(filepath + "filelog.bin", ios::in | ios::binary);
    if (!file_log) {
        cout << "open filelog.bin failed or maybe not exist!" << endl;
    }
    // file_log 存储的是元数据信息 (active_file_cnt_(4B) + ..)
    file_log.read((char *)&active_file_cnt_, sizeof(int));
    file_log.close();
    if (active_file_cnt_ == 0) {
        // 如果为0, 则说明没有数据文件, 创建一个
        active_file_cnt_ = 1;
        // 以二进制写入模式打开, 仅追加写
        file_log.open(filepath + "filelog.bin", ios::out | ios::binary | ios::app);
        file_log.write((char *)&active_file_cnt_, sizeof(int));
        file_log.close();
        cout << "create file filelog.bin successfully" << endl;
        return ;
    }
    start_ = true;
}

uint32_t bitcask::crc32(string value) {
    boost::crc_32_type result;
    result.process_bytes(value.data(), value.size());
    return result.checksum();
}

void bitcask::insert_data(string key, string value) {
    // 读索引，判断key是否存在
    bitcask_index find;
    find = read_index(key);
    if (find.key != "") {
        cout << "key: " +  key + " already exists" << endl;
        return update_data(key, value);
    }
    // 添加数据  
    bitcask_data new_data;
    new_data.key = key;
    new_data.key_size = int(key.size());
    new_data.value = value;
    new_data.value_size = int(value.size());
    new_data.timestamp = boost::posix_time::microsec_clock::universal_time();
    new_data.crc = crc32(value);
    // 添加索引
    fstream data_file;
    bitcask_index new_index;
    new_index.key = key;
    new_index.file_id = file_prev + to_string(active_file_cnt_);
    data_file.open(filepath + new_index.file_id, ios::out | ios::binary | ios::app);
    if (!data_file) {
        cout << cmd_prompt + new_index.file_id + " open failed" << endl;
    }
    new_index.value_size = sizeof(new_data);
    new_index.value_pos = data_file.tellp(); // 当前data_file写指针的位置
    new_index.timestamp = new_data.timestamp;
    new_index.value_valid = true;
    if (new_index.value_pos > file_max || file_max - new_index.value_pos < sizeof(new_data)) {
        // 如果写文件尾部的空间不足, 则创建新的数据文件
        active_file_cnt_++;
        new_index.file_id = file_prev + to_string(active_file_cnt_);
        new_index.value_pos = 0;    
    }
    data_file.close();
    // 写入数据到 cur active file
    write_data(new_data);
    // 写入索引 hint.bin, 更新内存中的hash_index
    write_index(new_index);
    hash_index[key] = new_index;
}

void bitcask::write_data(bitcask_data data) {
    fstream data_file;
    string cur_acvitve_file = file_prev + to_string(active_file_cnt_);
    data_file.open(filepath + cur_acvitve_file, ios::out | ios::binary | ios::app);
    if (!data_file) {
        cout << cmd_prompt + "[Write Data]" + cur_acvitve_file + " open failed" << endl;
    }
    boost::archive::binary_oarchive oa(data_file, boost::archive::no_header);
    oa << data;
    data_file.close();
}

void bitcask::write_index(bitcask_index index) {
    fstream hint;
    hint.open(filepath + "hint.bin", ios::out | ios::binary | ios::app);
    if (!hint) {
        cout << cmd_prompt + "[Write Index]"+ "hint.bin open failed" << endl;
    }
    boost::archive::binary_oarchive oa(hint, boost::archive::no_header);
    oa << index;
    hint.close();
}

bitcask_index bitcask::read_index(string key) {
    bitcask_index find;
    if (key == "") {
        return find;
    }
    if (hash_index.find(key) != hash_index.end()) {
        find = hash_index[key];
    }
    return find;
}

bitcask_data bitcask::read_data(string key) {
    bitcask_data data;
    bitcask_index find;
    find = read_index(key);
    if (find.value_valid) {
        fstream data_file;
        data_file.open(filepath + find.file_id, ios::in | ios::binary);
        if (!data_file) {
            cout << cmd_prompt + "[Read Data]" + find.file_id + " open failed" << endl;
        }
        data_file.seekg(find.value_pos, ios::beg);
        boost::archive::binary_iarchive ia(data_file, boost::archive::no_header);
        ia >> data;
        data_file.close();
    }
    return data;
}

void bitcask::read_datainfo(string key) {
    bitcask_data data;
    bitcask_index find;
    data = read_data(key);
    find = read_index(key);
    if (find.value_valid) {
        respone_ += cmd + "key: " + data.key + "\n";
        respone_ += cmd + "value: " + data.value + "\n";
        respone_ += cmd + "crc: " + to_string(data.crc) + "\n";
        respone_ += cmd + "file_id: " + find.file_id + "\n";
        respone_ += cmd + "value_size: " + to_string(find.value_size) + "\n";
        respone_ += cmd + "value_pos: " + to_string(find.value_pos) + "\n";
        respone_ += cmd + "timestamp: " + to_simple_string(find.timestamp) + "\n";
    } else {
        cout << "key: " << key << " not found" << endl;
    }
    return ;
}

void bitcask::delete_data(string key) {
    bitcask_index find;
    find = read_index(key);
    if (find.key == "") {
        cout << "delete key: " << key << " not found" << endl;
        respone_ += cmd_prompt + "delete key: " + key + " not found" + "\n";
        return ;
    }
    find.value_valid = false;
    hash_index[key] = find;
    // write_index(find);
}

void bitcask::update_data(string key, string value) {
    bitcask_index find;
    find = read_index(key);
    if (find.key == "") {
        cout << "update key: " << key << " not found" << endl;
        respone_ += cmd_prompt + "update key: " + key + " not found" + "\n";
        return ;
    }
    // update data
    bitcask_data new_data;
    new_data.key = key;
    new_data.key_size = int(key.size());
    new_data.value = value;
    new_data.value_size = int(value.size());
    new_data.timestamp = boost::posix_time::microsec_clock::universal_time();
    new_data.crc = crc32(value);

    // update index
    find.file_id = file_prev + to_string(active_file_cnt_);
    find.value_size = sizeof(new_data);
    find.timestamp = new_data.timestamp;
    find.value_valid = true;
    fstream data_file;
    data_file.open(filepath + find.file_id, ios::out | ios::binary | ios::app);
    if (!data_file) {
        cout << cmd_prompt + find.file_id + " open failed" << endl;
        respone_ += cmd_prompt + find.file_id + " open failed" + "\n";
    }
    find.value_pos = data_file.tellp();
    if (find.value_pos > file_max || file_max - find.value_pos < sizeof(new_data)) {
        active_file_cnt_++;
        find.file_id = file_prev + to_string(active_file_cnt_);
        find.value_pos = 0;
    }
    data_file.close();
    write_data(new_data);
    update_index(find, key);
    respone_ += cmd_prompt + "update key: " + key + " successfully" + "\n";
}

void bitcask::update_index(bitcask_index index, string key) {
    hash_index[key] = index;
}

void bitcask::merge()
{
    int beans = 1;
    long value_pos;
    vector<bitcask_data> data_array;
    for (; beans <= active_file_cnt_; beans++)
    {
        string file = file_prev + to_string(beans);
        fstream datafile;
        datafile.open(filepath + file, ios::binary | ios::in);
        
        if (!datafile)
        {
            respone_ += cmd_prompt + "the data file " + file + " open failure!\n";
            continue;
        }
        bitcask_data beans_data;
        bitcask_index beans_index;
        datafile.seekg(0, ios::beg);
        while (datafile)
        {
            if (datafile.peek() == EOF) {
                goto do_merge; // 检查是否到达文件末尾
            }
            boost::archive::binary_iarchive ia(datafile, boost::archive::no_header);
            try
            {
                ia >> beans_data;
                beans_index = read_index(beans_data.key);
            }
            catch (const std::exception &e)
            {
                std::cerr << "[do_merge]Exception during deserialization: " << e.what() << std::endl;
                goto do_merge;
            }

            if (beans_index.value_valid == true && beans_data.timestamp == beans_index.timestamp)
            {
                data_array.push_back(beans_data);
            }
        }

    do_merge:
        datafile.close();
        datafile.open(filepath + file, ios::binary | ios::out | ios::app);
        if (!datafile)
        {
            respone_ += cmd_prompt + "the data file " + file + " open failure!\n";
            continue;
        }
        datafile.seekg(0, ios::beg);

        for (bitcask_data data : data_array)
        {
            value_pos = datafile.tellg();
            boost::archive::binary_oarchive oa(datafile, boost::archive::no_header);
            try
            {
                oa << data;
            }
            catch (const std::exception &e)
            {
                std::cerr << "Exception during serialization: " << e.what() << std::endl;
                continue;
            }
            bitcask_index seaindex = read_index(data.key);
            seaindex.value_pos = value_pos;
            hash_index[data.key] = seaindex;
        }
        datafile.close();
        data_array.clear();
    }

    if (active_file_cnt_ >= 2)
    {
        while (active_file_cnt_ > 1)
        {
            string file = file_prev + to_string(active_file_cnt_);
            fstream datafile;
            datafile.open(filepath + file, ios::binary | ios::in);
            if (!datafile)
            {
                respone_ += cmd_prompt + "the data file " + file + " open failure!\n";
                continue;
            }
            bitcask_data beans_data;
            bitcask_index beans_index;
            datafile.seekg(0, ios::beg);
            while (datafile)
            {
                if (datafile.peek() == EOF) {
                    break; // 检查是否到达文件末尾
                }
                boost::archive::binary_iarchive ia(datafile, boost::archive::no_header);
                try
                {
                    ia >> beans_data;
                }
                catch (const std::exception &e)
                {
                    std::cerr << "[do_merge_2]Exception during deserialization: " << e.what() << std::endl;
                    goto do_merge_2;
                }

                data_array.push_back(beans_data);
            }

        do_merge_2:
            datafile.close();
            for (int pos = 1; pos < active_file_cnt_; pos++)
            {
                string mergefile = file_prev + to_string(pos);
                fstream datafile;
                datafile.open(filepath + mergefile, ios::binary | ios::out | ios::app);
                if (!datafile)
                {
                    respone_ += cmd_prompt + "the data file " + file + " open failure!\n";
                    continue;
                }
                long mergefile_end = datafile.tellg();
                bitcask_data merge_data = data_array.back();
                bitcask_index merge_index = read_index(merge_data.key);
                while (mergefile_end < file_max && (file_max - mergefile_end) < sizeof(merge_data))
                {
                    boost::archive::binary_oarchive oa(datafile, boost::archive::no_header);
                    try
                    {
                        oa << merge_data;
                    }
                    catch (const std::exception &e)
                    {
                        std::cerr << "Exception during serialization: " << e.what() << std::endl;
                        continue;
                    }
                    merge_data = data_array.back();
                    merge_index = read_index(merge_data.key);
                    merge_index.value_pos = mergefile_end;
                    merge_index.file_id = mergefile;
                    hash_index[merge_data.key] = merge_index;
                    mergefile_end += sizeof(merge_data);
                    data_array.pop_back();
                    merge_data = data_array.back();
                }
                datafile.close();
            }
            if (data_array.size() == 0)
            {
                active_file_cnt_--;
            }
            else
            {
                fstream newdatafile;
                newdatafile.open(filepath + file, ios::binary | ios::out);
                if (!newdatafile)
                {
                    respone_ += cmd_prompt + "the data file " + file + " open failure!\n";
                    continue;
                }
                newdatafile.seekg(0, ios::beg);
                for (auto data : data_array)
                {
                    boost::archive::binary_oarchive oa(newdatafile, boost::archive::no_header);
                    try
                    {
                        oa << data;
                    }
                    catch (const std::exception &e)
                    {
                        std::cerr << "Exception during serialization: " << e.what() << std::endl;
                        continue;
                    }
                }
                newdatafile.close();
                break;
            }
        }
    }
}

void bitcask::flush() {
    fstream file_log;
    file_log.open(filepath + "filelog.bin", ios::out | ios::binary);
    if (!file_log) {
        cout << "open filelog.bin failed or maybe not exist!" << endl;
        respone_ += cmd_prompt + "open filelog.bin failed or maybe not exist!" + "\n";
    }
    file_log.write((char *)&active_file_cnt_, sizeof(int));
    fstream hint;
    hint.open(filepath + "hint.bin", ios::out | ios::binary);
    if (!hint) {
        cout << "open hint.bin failed or maybe not exist!" << endl;
        respone_ += cmd_prompt + "open hint.bin failed or maybe not exist!" + "\n";
    }
    hint.seekg(0, ios::beg);
    for (auto index : hash_index) {
        if (index.second.value_valid == false) {
            continue;
        }
        boost::archive::binary_oarchive oa(hint, boost::archive::no_header);
        oa << index.second;
    }
}

string bitcask::Get(string key) {
    if (this->start_ == false) {
        cout << "bitcask not start, please open db first" << endl;
        return "";
    }
    bitcask_data data;
    bitcask_index find;
    data = read_data(key);
    find = read_index(key);
    if (find.value_valid) {
        return data.value;
    } else {
        cout << "key: " << key << " not found" << endl;
        return "";
    }
}

void bitcask::Put(string key, string value) {
    if (this->start_ == false) {
        cout << "bitcask not start, please open db first" << endl;
        return ;
    }
    insert_data(key, value);
}

void bitcask::Open(string path) {
    if (this->start_ == true) {
        cout << "bitcask already start, please close db first" << endl;
        return ;
    }
    init(path);
}

void bitcask::Close() {
    if (this->start_ == false) {
        cout << "bitcask not start, please open db first" << endl;
        return ;
    }
    merge();
    flush();
    this->start_ = false;
    active_file_cnt_ = 0;
    finish_ = false;
    respone_ = "";
    filepath = "";
}

void bitcask::Show() {
    cout << respone_ << endl;
}