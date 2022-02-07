#include <gtest/gtest.h>
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include <stdlib.h>

const char *dbfile_dir = "db/";
const char *tpch_dir ="tpch/";
const char *catalog_path = "catalog";


using namespace std;
DBFile dataFile;

// Test DBFile operation Create with heap parameter
TEST(DBFileTest, CreateTest) {
    EXPECT_EQ(1, dataFile.Create("tests/test.bin", heap, NULL));
}

// Test DBFile operation Create without heap parameter
TEST(DBFileTest, CreateTestWrongParameter) {
    EXPECT_EQ(0, dataFile.Create("tests/test.bin", tree, NULL));
}

// Test DBFile operation Open
TEST(DBFileTest, OpenTest) {
    EXPECT_EQ(dataFile.Open("tests/test.bin"), 1);
}

// Test DBFile operation Open
TEST(DBFileTest, CloseTest) {
    EXPECT_EQ(dataFile.Close(), 1);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}