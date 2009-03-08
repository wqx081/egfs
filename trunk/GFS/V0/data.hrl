%% Author: zyb@fit
%% Created: 2009-3-9

%% data:
%% 
%% TOTAL_DATA_SIZE = 2^31 MBytes
%% CHUNK_CAPACITY = 2^6 MBytes
%% TOTAL_CHUNK_RECORD_NUMBER = TOTAL_DATA_SIZE / CHUNK_CAPACITY = 2^25
%% CHUNKSERVER_CAPACITY = 2^10 MBytes
%% CHUNKSERVER_NUMBER = 2^11 
%% 
%% CHUNK_RECORD_SIZE = 96 bit = 12 Bytes


-define(TOTAL_DATA_SIZE,2147483648).   %% MBytes   , math:pow(2,31)
-define(CHUNK_CAPACITY,64).   %% MBytes  ,math:pow(2,6)
-define(TOTAL_CHUNK_RECORD_NUMBER,?TOTAL_DATA_SIZE/?CHUNK_CAPACITY). %% 2^25 =  33554432
-define(CHUNKSERVER_CAPACITY,1048576).   %% MBytes ,math:pow(2,20)
-define(CHUNKSERVER_NUMBER,2048).  %% math:pow(2,11)
-define(CHUNK_RECORD_SIZE,96). %%Bits


%% network

-define(LATENCY,1). %% millisecond
-define(BANDWIDTH,100). %% Mbps = 1000,000 bits?

%% bloom filter
-define(BLOOM_RECORD_SIZE,9.6).