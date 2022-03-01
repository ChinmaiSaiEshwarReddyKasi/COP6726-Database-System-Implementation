#include <gtest/gtest.h>
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include "BigQ.h"
#include <stdlib.h>

char *nationFile = "db/nation.bin";
char *tempFile ="db/test.bin";


using namespace std;
File f;
Page page;
int idx = 0;
DBFile dataFile;
ComparisonEngine ce;
Schema* s = new Schema("catalog", "nation");
OrderMaker* o = new OrderMaker(s);

TEST(BigQTest, TestRecordPipeline) {
    f.Open(1, nationFile);
    priority_queue<Record*, vector<Record*>, CompareRecords> bufferQ (o);
    f.GetPage(&page, 0);
    Record* r = new Record;
    while (page.GetFirst(r)) {
        bufferQ.push(r);
        r = new Record;
    }

    File tmp;
    tmp.Open(0, nationFile);
    Page tmpPage;
    priority_queue<class TPMMS*, vector<class TPMMS*>, CompareBuffers> tmpBufferQ (o);
    tpmmsPipeline(bufferQ, tmpBufferQ, f, page, idx);
    EXPECT_EQ(bufferQ.size(), 0);
    EXPECT_EQ(tmpBufferQ.size(), 1);
    f.Close();
}

TEST(BigQTest, TestUpdatingFirstRecord) {
    Record r;
    f.Open(1, nationFile);
    class TPMMS* tpmms = new class TPMMS(&f, 0, 1);
    f.GetPage(&page, 0);
    page.GetFirst(&r);
    while (page.GetFirst(&r)) {
        EXPECT_EQ(tpmms->firstRecordUpdate(), 1);
    }
    f.Close();
}

TEST(BigQTest, TestComparingRecords) {
    f.Open(1, nationFile);
    priority_queue<Record*, vector<Record*>, CompareRecords> recQ (o); 

    f.GetPage(&page, 0);
    Record* r = new Record;
    while (page.GetFirst(r)) {
        recQ.push(r);
        r = new Record;
    }

    page.EmptyItOut();
    f.GetPage(&page, 0);
    Record tmpRec[2];
    Record *end = NULL, *before = NULL;
    int i = 0;
    while (page.GetFirst(&tmpRec[i%2]) == 1) {
        before = end;
        end = &tmpRec[i%2];
        if (before && end) {
            EXPECT_EQ(ce.Compare(before, end, o), -1);
        }
        i++;
    }
    f.Close();
}

TEST(BigQTest, TestComparingBuffers) {
    f.Open(1, "db/lineitem.bin");
    priority_queue<class TPMMS*, vector<class TPMMS*>, CompareBuffers> bufferQ (o);
    ComparisonEngine comparisonEngine;

    class TPMMS* temp1 = new class TPMMS(&f, 0, 1);
    class TPMMS* temp2 = new class TPMMS(&f, 1, 1);
    bufferQ.push(temp1);
    bufferQ.push(temp2);

    Record temp1Top, temp2Top;
    temp1Top.Copy(bufferQ.top()->firstRecord);
    bufferQ.pop();
    temp2Top.Copy(bufferQ.top()->firstRecord);
    bufferQ.pop();
    EXPECT_EQ(ce.Compare(&temp1Top, &temp2Top, o), -1);

    f.Close();
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}