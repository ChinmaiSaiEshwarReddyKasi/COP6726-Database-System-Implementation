#include <iostream>
#include <gtest/gtest.h>
#include <map>
#include <vector>
#include <iostream>
#include <assert.h>
#include <string.h>

#include "ParseTree.h"
#include "Statistics.h"
#include "NodeDataOperations.h"
#include "NodeDataQuery.h"

extern "C"
{
int yyparse(void);
struct YY_BUFFER_STATE* yy_scan_string(const char*);

}
extern struct FuncOperator* finalFunction;
extern struct TableList* tables;
extern struct AndList* boolean;
extern struct NameList* groupingAtts;
extern struct NameList* attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

using namespace std;

class Project4Tests : public ::testing::Test {};

TEST_F(Project4Tests, TestJoinAndSelectOperations) {
    char* cnf = "SELECT c.c_name FROM customer AS c, nation AS n, region AS r WHERE(c.c_nationkey = n.n_nationkey) AND (n.n_regionkey = r.r_regionkey)";
    yy_scan_string(cnf);
    yyparse();
    NodeDataOperations *NodeDataOperations;

    vector<AndList> joinlist, havingList, whereList;
    Statistics* statistics = new Statistics();
    NodeDataOperations->getOperationsData(joinlist, havingList, whereList,  *statistics);

    ASSERT_TRUE(joinlist.size() == 2 && whereList.size() == 0);
    delete NodeDataOperations;
}

TEST_F(Project4Tests, TestGettingTableData) {
    NodeDataOperations *NodeDataOperations;

    Statistics* statistics = new Statistics();
    statistics->Read("Statistics.txt");
    vector<string> r;
    NodeDataOperations->getTableData(r);

    ASSERT_EQ(r.size(), 3);
    delete NodeDataOperations;
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}