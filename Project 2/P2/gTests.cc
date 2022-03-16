#include "gtest/gtest.h"
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include "DBFile.cc"
#include "HeapDBFile.cc"
#include "SortedDBFile.cc"
#include "SortedDBFile.h"
#include "BigQ.h"
#include "BigQ.cc"
#include <stdlib.h>
#include "Defs.h"
using namespace std;

class Project2Test : public ::testing::Test {
protected:
    DBFile* d;
    OrderMaker om;
    Project2Test() { }
    void SetUp() override {
        d = new DBFile();
        om.numAtts = 1;
        om.whichAtts[0] = 4;
        om.whichTypes[0] = String;
    }
    
};

TEST_F(Project2Test, TestCreatingFile) {
    struct {OrderMaker *o; int l;} s = {&om, 16};
    int res = d->Create("./gtest.bin", sorted, &s);
    d->Close();
    EXPECT_EQ(res, 1);
}

TEST_F(Project2Test, TestCreatingAndClosingFile) {
    struct {OrderMaker *o; int l;} s = {&om, 16};
    d->Create("./gtest.bin", sorted, &s);
    EXPECT_EQ(d->Close(), 1);
}

TEST_F(Project2Test, TestOpeningClosingFile) {
    d->Open("./gtest.bin");
    EXPECT_EQ(d->Close(), 1);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
