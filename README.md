# PACTree 

This repository contains the artifact for the SOSP'21 paper:

*"PACTree: A High Performance Persistent Range Index Using PAC Guidelines"    
Wook-Hee Kim, R. Madhava Krishnan, Xinwei Fu, Sanidhya Kashyap, and Changwoo Min  
In Proceedings of the 28th ACM Symposium on Operating Systems Principles (ACM SOSP 2021)*


## Directory structure
```{.sh}
pactree
├── include             # public headers of pactree 
├── lib                 # PDL-ART 
├── src                 # pactree source code 
├── tools               # misc build tools
```
## System configuration 
- Fedora 29 or higher //  Ubuntu 18.04 or higher
- x86-64 CPU supporting AVX512 instructions
```
# You can check using the following command:
$ cat /proc/cpuinfo | egrep -ho 'avx[^ ]*' | sort -u
```
- gcc 7.0 or higher
- at least 30 GB for each partition of NVM or SSD


## Dependency
### Install packages
- Install latest version of CMake
``` 
1. Download latest version from https://cmake.org/download/
$ tar -xvf cmake-your_version.tar.gz
$ cd cmake-your-version
$ ./bootstrap
$ make 
$ sudo make install
```
- Install other packages
```
#Ubuntu
$ sudo apt-get install g++ libtbb-dev libjemalloc-dev libnuma-dev libpmem-dev libpmemobj-dev python zlib1g-dev libboost-dev 

#Felodra
$ sudo yum install cmake make gcc-c++ tbb-devel jemalloc-devel numactl-devel libpmem-devel libpmemobj-devel python3 zlib-devel boost-devel 
```

## Environment setting for actual NVM
### Mount dax file systems
```
$ sudo mkfs.ext4 -b 4096 -E stride=512 -F /dev/pmem0
$ sudo mount -o dax /dev/pmem0 /mnt/pmem0

$ sudo mkfs.ext4 -b 4096 -E stride=512 -F /dev/pmem1
$ sudo mount -o dax /dev/pmem0 /mnt/pmem1
```

## Environment setting for non-NVM
You can use SSD instead of acutal NVM.
```
# vi ~/.bashrc and add following:
export PMEM_IS_PMEM_FORCE=1
# This is for eliminating msync overhead.  
```
```
source ~/.bashrc
```
```
$ sudo mkdir /mnt/pmem0
$ sudo chmod 777 /mnt/pmem0

$ sudo mkdir /mnt/pmem1
$ sudo chmod 777 /mnt/pmem1
```

## How to compile
```
$ bash ./tools/get-numa-config.sh
$ mkdir build
$ cd build
$ cmake ..
$ make
```
You can find PDL-ART library(libpdlart.a) at build/lib/PDL-ART.  
You can find PACTree library(libpactree.a) at build/src/ . 

## PACTree Configuration 
```
$ vi include/common.h
#define STRINGKEY    // Set String key type
#define KEYLENGTH 32 // Set String key length (current : 32)
#define MULTIPOOL    // Use multiple NUMA Persistent Memory Pool 
```
## Example
If you want to use PACTree for your project, please follow below:
```
1. Make your project directory
2. Add your project directory to the CMakeLists.txt
3. Write CMakeLists.txt for your project to your project directory
```
Please refer ./CMakeLists.txt, example/example.cpp, example/CMakeLists.txt.  
You can find the example at build/example/.  
