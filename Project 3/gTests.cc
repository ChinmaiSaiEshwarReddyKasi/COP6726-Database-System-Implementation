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
#include "RelOp.cc"
#include "RelOp.h"
#include "Function.h"
#include <stdlib.h>
#include "Defs.h"
using namespace std;

class Project3Tests : public ::testing::Test {};

TEST_F(Project3Tests, TestSelectFileInit) {
    SelectFile selectFile;
}

TEST_F(Project3Tests, TestSelectFileWaitUntillDone) {
    SelectFile selectFile;
    selectFile.WaitUntilDone();
}

TEST_F(Project3Tests, TestGroupBy) {
    GroupBy* gb = new GroupBy();
    gb->Use_n_Pages(100);
    EXPECT_EQ(100, gb->pages);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
