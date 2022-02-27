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


class TPMMS {

public:
	TPMMS(File* f, int firstPage, int runLength);
	//Used to update the top record of current run
	int firstRecordUpdate();
	//top record of current Run
	Record *firstRecord; 

private: 
	File* fBase;
	int runLength;
	Page bP;
	int firstPage;
	int presentPage;
};

//Class used for comparing records for given CNF
class CompareRecords {

public:
	bool operator () (Record* left, Record* right);
	CompareRecords(OrderMaker *sequence);

private:
	OrderMaker *sequence;

};

//Class used for comparing run for given CNF
class CompareBuffers {

public:
	bool operator () (TPMMS* left, TPMMS* right);
	CompareBuffers(OrderMaker *sequence);

private:
	OrderMaker *sequence;

};

//Struct used as arguement for worker thread
typedef struct tpmmsStruct {
	OrderMaker *sequence;
	Pipe *inputData;
	Pipe *outputData;
	int runlength;
	
};

//Main method executed by worker, worker will retrieve records from input pipe, 
//sort records into runs and puting all runs into priority queue, and geting sorted reecords
//from priority queue to output pipe
void* tpmmsMainProcess(void* arg);

//Used for take sequences of pages of records, and construct a run to hold such records, and put run
//into priority queue
void* tpmmsPipeline(priority_queue<Record*, vector<Record*>, CompareRecords>& queueRecord, 
    priority_queue<TPMMS*, vector<TPMMS*>, CompareBuffers>& queueRunner, File& f, Page& bP, int& indexOfPage);

#endif
