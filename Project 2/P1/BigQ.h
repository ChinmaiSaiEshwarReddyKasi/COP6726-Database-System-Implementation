#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include "File.h"
#include "Record.h"
#include <iostream>
#include <queue>
#include "Pipe.h"


using namespace std;

class BigQ {

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

};

//TPMMS struct used as arguement for pthread
typedef struct tpmmsStruct {
	OrderMaker *sequence;
	Pipe *inputData;
	Pipe *outputData;
	int runlength;
	
};


class TPMMS {

public:
	TPMMS(File* f, int firstPage, int runLength);
	int firstRecordUpdate();
	Record *firstRecord; 

private: 
	File* fBase;
	int runLength;
	Page bP;
	int firstPage;
	int presentPage;
};

//Used for comparing records
class CompareRecords {

public:
	bool operator () (Record* left, Record* right);
	CompareRecords(OrderMaker *sequence);

private:
	OrderMaker *sequence;

};

//Used for comparing buffers
class CompareBuffers {

public:
	bool operator () (TPMMS* left, TPMMS* right);
	CompareBuffers(OrderMaker *sequence);

private:
	OrderMaker *sequence;

};

//TPMMS main process, which will get all records, club them and sort them.
void* tpmmsMainProcess(void* arg);

//Used for creating pipelines to hold records to compare.
void* tpmmsPipeline(priority_queue<Record*, vector<Record*>, CompareRecords>& queueRecord, priority_queue<TPMMS*, vector<TPMMS*>, CompareBuffers>& queueRunner, 
File& f, Page& bP, int& indexOfPage);

#endif
