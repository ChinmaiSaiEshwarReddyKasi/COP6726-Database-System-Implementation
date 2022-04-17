#ifndef DBFILE_H
#define DBFILE_H

#include "BigQ.h"
#include "Comparison.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "File.h"


struct SortInfo {
    OrderMaker *myOrder;
    int runLength;
};

typedef enum {
    heap,
    sorted
} fType;


class GenericDBFile {
protected:
    File *file;
    char *fpath;
    off_t currentPageIndex;
    Page *currentPage;
    bool writingMode;

public:
    virtual int Create (const char *fpath) = 0;
    virtual int Open (char *fpath) = 0;
    virtual void MoveFirst () = 0;
    virtual void Load (Schema &myschema, const char *loadpath) = 0;
    virtual void Add (Record &addme) = 0;
    virtual int GetNext (Record &fetchme) = 0;
    virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
    virtual int Close () = 0;
    virtual ~GenericDBFile ();
};


class DBFile {
private:
    GenericDBFile *myInternalVar;

public:
    DBFile ();
    ~DBFile ();

    int Create (const char *fpath, fType ftype, void *startup);
    int Open (char *fpath);
    void MoveFirst ();
    void Load (Schema &myschema, const char *loadpath);
    void Add (Record &addme);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    int Close ();
};

class Heap : public GenericDBFile {

public:
    Heap ();
    ~Heap ();
    int Create (const char *fpath);
    int Open (char *fpath);
    void MoveFirst ();
    void Load (Schema &myschema, const char *loadpath);
    void Add (Record &addme);
    void FlushCurrentPage();
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    int Close ();

};

class Sorted : public GenericDBFile {
private:
    Pipe *inPipe, *outPipe;
    BigQ *bigq;
    OrderMaker *query, *order;
    int buffsize, runLength;

public:
    Sorted (OrderMaker *order, int runLength);
    ~Sorted ();
    int Create (const char *fpath);
    int Open (char *fpath);
    void InitBigQ ();
    void MoveFirst ();
    void Load (Schema &myschema, const char *loadpath);
    void Add (Record &addme);
    void TwoWayMerge ();
    int BinarySearch(Record &fetchme, CNF &cnf, Record &literal);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    int GetNextInSequence (Record &fetchme, CNF &cnf, Record &literal);
    int GetNextOfQuery (Record &fetchme, CNF &cnf, Record &literal);
    int QueryOrderMaker (OrderMaker &query, OrderMaker &order, CNF &cnf);
    int Close ();
};
#endif