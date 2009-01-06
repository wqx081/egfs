#!/bin/bash

gcc -o zerot zerot.c 

rm -rf ds
mkdir ds

# avi 700M
./zerot -f ds/avi -s 734003200 -n 1

# rar 100M*2=200M
./zerot -f ds/rar -s 104857600 -n 2

# mp3 5M*20 = 100M
./zerot -f ds/mp3 -s 5242880 -n 20

# jpeg 512K*40 = 20M
./zerot -f ds/jpg -s 524288 -n 40

# exe  100K*10 = 1M
./zerot -f ds/exe -s 102400 -n 10

# htm  10K *100 = 1M
./zerot -f ds/htm -s 10240 -n 100

# txt  2k * 512 = 1M 
./zerot -f ds/txt -s 2048 -n 512

# cpp  1k * 1024 = 1M
./zerot -f ds/cpp -s 1025 -n 1024 


