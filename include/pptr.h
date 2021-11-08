// SPDX-FileCopyrightText: Copyright (c) 2019-2021 Virginia Tech
// SPDX-License-Identifier: Apache-2.0

#include "pmem.h"


template <typename T>
class pptr {
    public:
	    //TEST
        pptr() noexcept : rawPtr{} {}
        pptr(int poolId, unsigned long offset){
            rawPtr = ((unsigned long)poolId) << 48 | offset;

        }
        T *operator->() {
           int poolId = (rawPtr&MASK_POOL) >> 48;
           void *baseAddr = PMem::getBaseOf(poolId); 
	   unsigned long offset = rawPtr & MASK;
	   return (T *)((unsigned long)baseAddr + offset);
        }

        T *getVaddr() {
	   unsigned long offset = rawPtr & MASK;
	   if(offset == 0){
               return nullptr;
		}

           int poolId = (rawPtr&MASK_POOL) >> 48;
           void *baseAddr = PMem::getBaseOf(poolId); 

	   return (T *)((unsigned long)baseAddr + offset);
        }
	
        unsigned long getRawPtr() {
		return rawPtr;
        }

        unsigned long setRawPtr(void *p) {
		rawPtr = (unsigned long)p;
        }

	inline void markDirty(){
		rawPtr= ((1UL << 61) | rawPtr);
	}

	bool isDirty(){
		return (((1UL << 61) & rawPtr)==(1UL<<61));
	}

	inline void markClean(){
		rawPtr= (rawPtr&MASK_DIRTY);
	}

    private:
        unsigned long rawPtr; // 16b + 48 b // nvm
};

