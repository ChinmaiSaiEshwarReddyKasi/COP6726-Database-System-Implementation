#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "HeapDBFile.h"
#include "Defs.h"
#include <iostream>
#include <fstream>
// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {

}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
//    cout<< "DBFile Create" << endl;
    char f_meta_name[100];
    sprintf (f_meta_name, "%s.meta", f_path);
    cout<<"meta data in "<<f_meta_name<<endl;
    ofstream f_meta;
    f_meta.open(f_meta_name);
    OrderMaker* orderMaker = nullptr;
    int runLength = 0;
    // write in file type
    if(f_type == heap){
        f_meta << "heap" << endl;
        genericDB = new HeapDBFile();
    }
    // else if(f_type==sorted){
    //     f_meta << "sorted"<< endl;
    //     genericDB = new DBFileSorted();
    // }
    // else if(f_type==tree){
    //     f_meta << "tree"<< endl;
    //     genericDB = new DBFileTree();
    // }
    // write in orderMaker and runLength
    if(startup!= nullptr) {
        SortedInfo* sortedInfo = ((SortedInfo*)startup);
        orderMaker = sortedInfo->myOrder;
        runLength = sortedInfo->runLength;
        f_meta << runLength << endl;
        f_meta << orderMaker->numAtts << endl;
        for (int i = 0; i < orderMaker->numAtts; i++) {
            f_meta << orderMaker->whichAtts[i] << endl;
            if (orderMaker->whichTypes[i] == Int)
                f_meta << "Int" << endl;
            else if (orderMaker->whichTypes[i] == Double)
                f_meta << "Double" << endl;
            else if(orderMaker->whichTypes[i] == String)
                f_meta << "String" << endl;
        }
        // store orderMaker and runLength into subclass
        if(f_type == heap){

        }
        // else if(f_type==sorted){
        //     ((DBFileSorted*)genericDB)->orderMaker = orderMaker;
        //     ((DBFileSorted*)genericDB)->runLength = runLength;
        // }
        else if(f_type==tree){

        }
    }
    f_meta.close();
    int res = genericDB->Create(f_path, f_type, startup);
    return res;
//    cout<< "end DBFile Create" << endl;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
    genericDB->Load(f_schema, loadpath);
}

int DBFile::Open (char *f_path) {
    OrderMaker* orderMaker = new OrderMaker();
    char f_meta_name[100];
    sprintf (f_meta_name, "%s.meta", f_path);
    ifstream f_meta(f_meta_name);

    string s;
    getline(f_meta, s);
    if(s.compare("heap")==0){
        genericDB = new HeapDBFile();
    }
    // else if(s.compare("sorted")==0){
    //     genericDB = new DBFileSorted();
    //     string temp;
    //     getline(f_meta, temp);
    //     int runLength = stoi(temp);
    //     temp.clear();
    //     getline(f_meta, temp);
    //     orderMaker->numAtts = stoi(temp);
    //     for(int i=0; i<orderMaker->numAtts; i++){
    //         temp.clear();
    //         getline(f_meta, temp);
    //         orderMaker->whichAtts[i] = stoi(temp);
    //         temp.clear();
    //         getline(f_meta, temp);
    //         if(temp.compare("Int")==0){
    //             orderMaker->whichTypes[i] = Int;
    //         }
    //         else if(temp.compare("Double")==0){
    //             orderMaker->whichTypes[i] = Double;
    //         }
    //         else if(temp.compare("String")==0){
    //             orderMaker->whichTypes[i] = String;
    //         }
    //     }
    //     ((DBFileSorted*)genericDB)->orderMaker = orderMaker;
    //     ((DBFileSorted*)genericDB)->runLength = runLength;
    //     orderMaker->Print();
    // }
    // else if(s.compare("tree")==0){
    //     genericDB = new DBFileTree();
    // }
    f_meta.close();
    int res = genericDB->Open(f_path);
    return res;
}

void DBFile::MoveFirst () {
    genericDB->MoveFirst();
}

int DBFile::Close () {
    int res = genericDB->Close();
    delete genericDB;
    return res;
}

void DBFile::Add (Record &rec) {
    genericDB->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
    return genericDB->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return genericDB->GetNext(fetchme, cnf, literal);
}
