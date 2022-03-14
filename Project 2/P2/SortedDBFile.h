//
// Created by LJJ on 2/24/21.
//

#ifndef A2_2TEST_DBFILESORTED_H
#define A2_2TEST_DBFILESORTED_H
#include "DBFile.h"
#include <queue>
#include "BigQ.h"


class SortedDBFile :public GenericDBFile{
    friend class DBFile;
private:
    File fileOnDisk;
    Page pageBuffer;
    off_t indexOfPage;
    int checkifWriting;
    char* way_Out = nullptr;
    int bCal = 0;
    int boundSmall;
    int boundLarge;

    Pipe* in = new Pipe(100);
    Pipe* out = new Pipe(100);
    pthread_t* thread = nullptr;

    OrderMaker* orderMaker = nullptr;
    int runLength;

    void writeMode();
    void readMode();
    static void *consumer (void *arg);
    int Run (Record *left, Record *literal, Comparison *c);
public:
    SortedDBFile ();

    int Create (char *file_path, fType file_type, void *begin);
    int Open (char *file_path);
    int Close ();

    void Load (Schema &mySch, char *path_Load);

    void MoveFirst ();
    void Add (Record &append);
    int GetNext (Record &bringBack);
    int GetNext (Record &bringBack, CNF &cnf, Record &literal);

};


#endif //A2_2TEST_DBFILESORTED_H
