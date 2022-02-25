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

// This function is used to create the db file to load the data.
int DBFile::Create (char *f_path, fType f_type, void *startup) {
    if(fileOpen){

        cerr << "The file is already opened. Cannot update.";
        return 0;

    }else if (f_type == heap)
    {
        // Opening the required file
        dataFile.Open(0, (char *) f_path);

        pageNumber = 0;
        stillWriting = false;
        fileOpen = true;
        return 1;
    }
     else {
        return 0;
    }
}

// This function is used to load the db file for processing.
void DBFile::Load (Schema &f_schema, char *loadpath) {
    if(!fileOpen){
        cerr << "Cannot load the db file as it is not opened";
        return;
    }
    FILE *textFile = fopen(loadpath, "r");
    if (textFile){
        Record r;

        // We iterarte over all the records in the file to get the data.
        while(r.SuckNextRecord(&f_schema, textFile)==1){
            Add(r);
        }
        if (stillWriting){
            dataFile.AddPage(&page, pageNumber);
        }
    } else {
        cout << "Error occured while opening the file.";
    }
    fclose(textFile);
}

// This function is used to open the db file.
int DBFile::Open (char *f_path) {
    if(fileOpen){
        cerr << "The file is already opened. Cannot update.";
        return 0;
    }
    dataFile.Open(1, (char *) f_path);
    fileOpen = true;
    stillWriting = false;
    pageNumber = 0;
    return 1;
}

void DBFile::MoveFirst () {
    if(!fileOpen){
        cerr << "Cannot move the cursor as the file is not opened";
        return;
    }    
    if(dataFile.GetLength() > 0){
        dataFile.GetPage(&page, 0);
    }
}

// This function is used to close the db file.
int DBFile::Close () {
    if(!fileOpen){
        cerr << "File not opened";
        return 0;
    }

    // Check if data is still being written then add data and exit.
    if(stillWriting){
        dataFile.AddPage(&page, pageNumber);
    }
    page.EmptyItOut();
    fileOpen = false;
    cout << "Closed the file.";
    cout << "The total length of the file was " << dataFile.Close() <<" pages." << "\n";
    return 1;
}

void DBFile::Add (Record &rec) {
    if(!fileOpen){
        cerr << "Cannot add to the db file as it is not opened";
        return;
    }
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
    if(!fileOpen){
        cerr << "Cannot get the next record from the db file as it is not opened";
        return 0;
    }
    if(stillWriting){ MoveFirst();}

    if(page.GetFirst(&fetchme) == 0){
        pageNumber += 1;
        if (pageNumber >= dataFile.GetLength() - 1){
            return 0;
        } else {
            page.EmptyItOut();
            dataFile.GetPage(&page, pageNumber);
            page.GetFirst(&fetchme);
        }
    }
    return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine ce;

    while(GetNext(fetchme)==1){
        if(ce.Compare(&fetchme, &literal, &cnf) == 1){return 1;}
    }
    return 0;
}
