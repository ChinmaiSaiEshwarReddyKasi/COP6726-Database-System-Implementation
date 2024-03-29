#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
	pthread_t thread;
	// Record *buffer;

	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {
    private:
        pthread_t thread;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Project : public RelationalOp { 
	private:
        pthread_t thread;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

void* ProjectOperation (void* arg);

typedef struct PArgs{
	Pipe *inPipe;
	Pipe *outPipe;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
};

class Join : public RelationalOp { 
	private:
        pthread_t thread;
		int runLen;
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

typedef struct JArgs{
	Pipe *inPipeL;
	Pipe *inPipeR;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	int runLen;
};

void* JoinMain(void* arg);

void JoinMergeRecords(Record* leftRecord, Record* rightRecord, Pipe* pipe);

void JoinMainMerge(JArgs* joinArg, OrderMaker* leftOrder, OrderMaker* rightOrder);

void NestedJoin(JArgs* joinArg);


class DuplicateRemoval : public RelationalOp {
	private:
		pthread_t thread;
		int runLen;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

void* RemoveDuplicates(void* arg);

typedef struct DRArgs{
	Pipe *inPipe;
	Pipe *outPipe;
	OrderMaker *order;
	int runLen;
};

class Sum : public RelationalOp {
    private:
        pthread_t thread;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class GroupBy : public RelationalOp {
    private:
        pthread_t thread;
	public:
    int pages = 16;
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class WriteOut : public RelationalOp {
	private:
		pthread_t thread;
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

void* WriteOutMain(void* arg);

typedef struct WOArgs{
	Pipe *inPipe;
	FILE *outFile;
	Schema *schema;
};

#endif
