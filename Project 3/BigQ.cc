#include "BigQ.h"
#include "DBFile.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    pthread_t thread;
    this->inputData = &in;
    this->outputData = &out;
    this->sequence = &sortorder;
    this->runlength = runlen;
    pthread_create(&thread, NULL, tpmmsMainProcess, (void*) this);
}

BigQ::~BigQ () {

}

TPMMS::TPMMS(File* f, int start, int l) {
    fBase = f;
	runLength = l;
    firstPage = start;    
    presentPage = start;
    firstRecord = new Record;
    fBase->GetPage(&bP, firstPage);
    firstRecordUpdate();
}

void* tpmmsMainProcess(void* arg) {
	((BigQ*) arg) -> tpmmsMainHandler();
	return NULL;
}

void BigQ::tpmmsMainHandler(){
    File f;
    Record r;
    Page bP;
    int countOfPage = 0;
    int indexOfPage = 0;
	priority_queue<Record*, vector<Record*>, CompareRecords> recordsQ (this->sequence);
    vector<Record* > RecordBuffer;
    priority_queue<TPMMS*, vector<TPMMS*>, CompareBuffers> buffersQ (this->sequence);
    char* fName = new char[100];
	sprintf(fName, "%d.bin", pthread_self());
	f.Open(0, fName);
    
    // TPMMS Phase 1 - Getting all records from the pipe
    while (this->inputData->Remove(&r) == 1) {
        Record* recordTemp = new Record;
        recordTemp->Copy(&r);
        // Check if the present page is full or not
        if (bP.Append(&r) == 0) {
            countOfPage++;
            bP.EmptyItOut();

            // Check if the present pipeline is full or not, and create a new one if full
            if (countOfPage == this->runlength) {
                tpmmsPipeline(recordsQ, buffersQ, f, bP, indexOfPage);
                recordsQ = priority_queue<Record*, vector<Record*>, CompareRecords> (this->sequence);
                countOfPage = 0;
            }

            bP.Append(&r);
        }

        recordsQ.push(recordTemp);
    }

    if (!recordsQ.empty()) {
        tpmmsPipeline(recordsQ, buffersQ, f, bP, indexOfPage);
        recordsQ = priority_queue<Record*, vector<Record*>, CompareRecords> (this->sequence);
    }
    // TPMMS Phase 2 - Gather all the buffers and merge them
	Schema s("catalog", "supplier");
    while (!buffersQ.empty()) {
        TPMMS* topData = buffersQ.top();
        buffersQ.pop();
		Record* wi = new Record();
		wi->Copy(topData->firstRecord);
		this->outputData->Insert(wi);
        if (topData->firstRecordUpdate() == 1) {
            buffersQ.push(topData);
        }
    }
    f.Close();
    this->outputData->ShutDown();
    remove(fName);
}


CompareBuffers::CompareBuffers(OrderMaker* orderMaker) {
    sequence = orderMaker;
}

bool CompareBuffers::operator () (TPMMS* l, TPMMS* r) {
    ComparisonEngine CE;
    if (CE.Compare(l->firstRecord, r->firstRecord, sequence) >= 0)
        return true;
    return false;
}

// Function to put the records in pipeline
void BigQ::tpmmsPipeline(priority_queue<Record*, vector<Record*>, CompareRecords>& recordsQ, priority_queue<TPMMS*, vector<TPMMS*>, CompareBuffers>& buffersQ, 
File& f, Page& bP, int& indexOfPage) {

    int indexOfStart = indexOfPage;
    bP.EmptyItOut();
    while (!recordsQ.empty()) {
        Record* recordTemp = new Record;
        recordTemp->Copy(recordsQ.top());
        recordsQ.pop();
        if (bP.Append(recordTemp) == 0) {
            f.AddPage(&bP, indexOfPage++);
            bP.EmptyItOut();
            bP.Append(recordTemp);
        }
    }
    f.AddPage(&bP, indexOfPage++);
    bP.EmptyItOut();
    TPMMS* tpmmsData = new TPMMS(&f, indexOfStart, indexOfPage - indexOfStart);
    buffersQ.push(tpmmsData);
}

int TPMMS::firstRecordUpdate() {
    //checking if the page is already full
    if (bP.GetFirst(firstRecord) == 0) {
        presentPage++;
        if (presentPage >= firstPage + runLength) {
            return 0;
        }
        bP.EmptyItOut();
        fBase->GetPage(&bP, presentPage);
        bP.GetFirst(firstRecord);
    }
    return 1;
}

CompareRecords::CompareRecords(OrderMaker* orderMaker) {
    sequence = orderMaker;
}

bool CompareRecords::operator () (Record* l, Record* r) {
    ComparisonEngine CE;
    if (CE.Compare(l, r, sequence) >= 0)
        return true;
    return false;
}
