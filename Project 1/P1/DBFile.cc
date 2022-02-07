#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "iostream"

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {

}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    if(f_type == heap){
        dataFile.Open(0, (char *) f_path);
        pageNumber = 0;
        stillWriting = false;
        return 1;
    } else {
        return 0;
    }
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE *textFile = fopen(loadpath, "r");
    if (textFile){
        Record r;
        while(r.SuckNextRecord(&f_schema, textFile)==1){
            Add(r);
        }
        if (stillWriting){
            dataFile.AddPage(&page, pageNumber);
        }
    } else {
        cout << "Error occurent when opening file.";
    }
    fclose(textFile);
}

int DBFile::Open (const char *f_path) {
}

void DBFile::MoveFirst () {
}

int DBFile::Close () {
    if(stillWriting){
        dataFile.AddPage(&page, pageNumber);
    }
    cout << "Closed the file.";
    cout << "The total length of the file was " << dataFile.Close() <<" pages." << "\n";
    return 1;
}

void DBFile::Add (Record &rec) {
    if(!stillWriting){
        page.EmptyItOut();
        if(dataFile.GetLength()){
            pageNumber = dataFile.GetLength() - 2;
            dataFile.GetPage(&page, pageNumber);
        }
        stillWriting = true;
    }
    if(page.Append(&rec) == 0){
        dataFile.AddPage(&page, pageNumber);
        pageNumber++;
        page.EmptyItOut();
        page.Append(&rec);
    }
}

int DBFile::GetNext (Record &fetchme) {
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
