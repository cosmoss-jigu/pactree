// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#ifndef PACTREE_COMMON_H
#define PACTREE_COMMON_H
#include <cstdint>
#include <string>
#include <cstring>
#include <cassert>
#include <iostream>
#include <libpmemobj.h>
#define MULTIPOOL
#define MAX_NUMA 1
//#define STRINGKEY
#define WORKER_THREAD_PER_NUMA 1
#define KEYLENGTH 32
//#define SYNC

//#define PACTREE_ENABLE_STATS


template <std::size_t keySize>
class StringKey {
private:
    char data[keySize];
    size_t keyLength = 0;
public:
    StringKey() { memset(data, 0x00, keySize);}
    StringKey(const StringKey &other) {
        memcpy(data, other.data, keySize);
    }
    StringKey(const char bytes[]) {set(bytes, strlen(bytes));}
    StringKey(int k) {
        setFromString(std::to_string(k));
    }
    inline StringKey &operator=(const StringKey &other) {
        memcpy(data, other.data, keySize);
//		keyLength = other.keyLength;
        return *this;
    }
    inline bool operator<(const StringKey<keySize> &other) { return strcmp(data, other.data) < 0;}
    inline bool operator>(const StringKey<keySize> &other) { return strcmp(data, other.data) > 0;}
    inline bool operator==(const StringKey<keySize> &other) { return strcmp(data, other.data) == 0;}
    inline bool operator!=(const StringKey<keySize> &other) { return !(*this == other);}
    inline bool operator<=(const StringKey<keySize> &other) { return !(*this > other);}
    inline bool operator>=(const StringKey<keySize> &other) {return !(*this < other);}

    size_t size() const {
        if (keyLength)
            return keyLength;
        else
            return strlen(data);
    }

    inline void setFromString(std::string key) {
        memset(data, 0, keySize);
        if (key.size() >= keySize) {
            memcpy(data, key.c_str(), keySize - 1);
            data[keySize - 1] = '\0';
            keyLength = keySize;
        } else {
            strcpy(data, key.c_str());
            keyLength = key.size();
        }
        return;
    }

    inline void set(const char bytes[], const std::size_t length) {
        assert(length <= keySize-1);
        memcpy(data, bytes, length);
 //       keyLength = length;
        data[length] = '\0';
    }
    const char* getData() const { return data;}
    //friend ostream & operator << (ostream &out, const StringKey<keySize> &k);
};

#ifdef STRINGKEY
typedef StringKey<KEYLENGTH> Key_t;
#else
typedef uint64_t Key_t;
#endif
typedef uint64_t Val_t;

class OpStruct {
public:
    enum Operation {dummy, insert, remove, done};
    Operation op; // 4 
    uint16_t poolId; //2
    uint8_t hash;  //1
    Key_t key;    //  8
    void* oldNodePtr; // old node_ptr 8
    PMEMoid newNodeOid; // new node_ptr 16
    Key_t newKey;  // new key ; 8
    Val_t newVal;// new value ; 8
    uint64_t ts; // 8
    bool operator< (const OpStruct& ops) const {
        return (ts < ops.ts);
    }
};

#endif 
