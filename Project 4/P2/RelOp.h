#ifndef REL_OP_H
#define REL_OP_H

#include <iostream>
#include <vector>
#include <cstring>
#include <sstream>
#include <pthread.h>

#include "Pipe.h"
#include "DBFile.h"
#include "Schema.h"
#include "Record.h"
#include "Function.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

class RelOp {
protected:
    pthread_t t;
	int runLen;
public:
	virtual void WaitUntilDone ();
	virtual void Start () = 0;
    virtual void Use_n_Pages (int n);
};

class SelectPipe : public RelOp {
private:
    CNF *cnf;
	Record *lit;
    Pipe *in, *out;
public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void Start ();
    SelectPipe () {};
    ~SelectPipe () {};
};

class SelectFile : public RelOp {
private:
    CNF *cnf;
	DBFile *file;
	Record *lit;
    Pipe *out;
public:
    void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
    SelectFile () {};
    void Start ();
    ~SelectFile () {};
};

class Project : public RelOp {
private:
    int numAttsIn, numAttsOut;
	Pipe *in, *out;
	int *attsToKeep;
public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
    Project () {};
    ~Project () {};
	void Start ();
};

class Join : public RelOp {
private:
    CNF *cnf;
    Record *lit;
	Pipe *inPipeL, *inPipeR, *out;
public:
    void Start ();
	~Join () {};
    Join () {};
	void Run (Pipe &inL, Pipe &inR, Pipe &outPipe, CNF &selOp, Record &literal);
};

class DuplicateRemoval : public RelOp {
private:
    Schema *schema;
	Pipe *in, *out;
public:
    void Start ();
    ~DuplicateRemoval () {};
	DuplicateRemoval () {};
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
};

class Sum : public RelOp {
private:
	Pipe *in, *out;
	Function *compute;
public:
    void Start ();
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
    ~Sum () {};
    Sum () {};
};

class GroupBy : public RelOp {
private:
	OrderMaker *order;
    Pipe *in, *out;
	Function *compute;
public:
    void Start ();
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
    GroupBy () {};
    ~GroupBy () {};
};

class WriteOut : public RelOp {
private:
    FILE *file;
	Pipe *in;
	Schema *schema;
public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
    ~WriteOut () {};
    WriteOut () {};
	void Start ();
};

#endif