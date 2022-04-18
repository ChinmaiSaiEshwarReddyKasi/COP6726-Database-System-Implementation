#include "gtest/gtest.h"
extern "C" {
	#include "y.tab.h"
}
#include <iostream>
#include <stdlib.h>
#include "Statistics.h"
#include "ParseTree.h"
#include <math.h>
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

using namespace std;

class Project4Tests : public ::testing::Test {};
char *statFileName = "Statistics.txt";

TEST_F(Project4Tests, TestingReadAndWriteOperation) {
	
	Statistics s;
    char *relName[] = {"supplier","partsupp"};

	
	s.AddRel(relName[0],10000);
	s.AddAtt(relName[0], "s_suppkey",10000);

	s.AddRel(relName[1],800000);
	s.AddAtt(relName[1], "ps_suppkey", 10000);	

	char *cnf = "(s_suppkey = ps_suppkey)";

	yy_scan_string(cnf);
	yyparse();
	double result = s.Estimate(final, relName, 2);
	
	ASSERT_NEAR (800000, result, 0.1);
	
	s.Apply(final, relName, 2);
	s.Write(statFileName);
	Statistics s1;
	s1.Read(statFileName);	
	cnf = "(s_suppkey>1000)";	
	yy_scan_string(cnf);
	yyparse();
	double dummy = s1.Estimate(final, relName, 2);
	
	ASSERT_NEAR (result, dummy * 3.0, 0.1);
	
}

TEST_F(Project4Tests, TestingStatisticsTestcase6) {

	Statistics s;
    char *relName[] = { "partsupp", "supplier", "nation"};

	s.Read(statFileName);
	
	s.AddRel(relName[0],800000);
	s.AddAtt(relName[0], "ps_suppkey",10000);

	s.AddRel(relName[1],10000);
	s.AddAtt(relName[1], "s_suppkey",10000);
	s.AddAtt(relName[1], "s_nationkey",25);
	
	s.AddRel(relName[2],25);
	s.AddAtt(relName[2], "n_nationkey",25);
	s.AddAtt(relName[2], "n_name",25);


	char *cnf = " (s_suppkey = ps_suppkey) ";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName, 2);
	
	cnf = " (s_nationkey = n_nationkey)  AND (n_name = 'AMERICA')   ";
	yy_scan_string(cnf);
	yyparse();

	double result = s.Estimate(final, relName, 3);
	
	ASSERT_NEAR (32000, result, 0.1);
	
	s.Apply(final, relName, 3);
	
	s.Write(statFileName);

}

TEST_F(Project4Tests, TestingStatisticsTestcase7) {
	
	Statistics s;
    char *relName[] = { "orders", "lineitem"};

	s.Read(statFileName);
	

	s.AddRel(relName[0],1500000);
	s.AddAtt(relName[0], "o_orderkey",1500000);
	
	
	s.AddRel(relName[1],6001215);
	s.AddAtt(relName[1], "l_orderkey",1500000);
	

	char *cnf = "(l_receiptdate >'1995-02-01' ) AND (l_orderkey = o_orderkey)";

	yy_scan_string(cnf);
	yyparse();
	double result = s.Estimate(final, relName, 2);
	
	ASSERT_NEAR (2000405, result, 0.1);
	
	s.Apply(final, relName, 2);
	s.Write(statFileName);
	
}

// Note  OR conditions are not independent.
TEST_F(Project4Tests, TestingStatisticsTestcase8) {
	
	Statistics s;
    char *relName[] = { "part",  "partsupp"};

	s.Read(statFileName);
	
	s.AddRel(relName[0],200000);
	s.AddAtt(relName[0], "p_partkey",200000);
	s.AddAtt(relName[0], "p_size",50);

	s.AddRel(relName[1],800000);
	s.AddAtt(relName[1], "ps_partkey",200000);
	

	char *cnf = "(p_partkey=ps_partkey) AND (p_size =3 OR p_size=6 OR p_size =19)";

	yy_scan_string(cnf);
	yyparse();
	
		
	double result = s.Estimate(final, relName,2);
	
	ASSERT_NEAR (48000, result, 0.1);
	
	s.Apply(final, relName,2);
	
	s.Write(statFileName);

}

TEST_F(Project4Tests, TestingStatisticsTestcase9) {
	
	Statistics s;
    char *relName[] = { "part",  "partsupp","supplier"};

	
	s.AddRel(relName[0],200000);
	s.AddAtt(relName[0], "p_partkey",200000);
	s.AddAtt(relName[0], "p_name", 199996);

	s.AddRel(relName[1],800000);
	s.AddAtt(relName[1], "ps_partkey",200000);
	s.AddAtt(relName[1], "ps_suppkey",10000);
	
	s.AddRel(relName[2],10000);
	s.AddAtt(relName[2], "s_suppkey",10000);
	
	char *cnf = "(p_partkey=ps_partkey) AND (p_name = 'dark green antique puff wheat') ";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName,2);
	
	cnf = " (s_suppkey = ps_suppkey) ";
	yy_scan_string(cnf);
	yyparse();

	double result = s.Estimate(final, relName,3);
	
	ASSERT_NEAR (4, result, 0.5);
	
	s.Apply(final, relName,3);
	
	s.Write(statFileName);

}

TEST_F(Project4Tests, TestingStatisticsTestcase10) {
	
	Statistics s;
    char *relName[] = { "customer", "orders", "lineitem","nation"};

	s.Read(statFileName);
	
	s.AddRel(relName[0],150000);
	s.AddAtt(relName[0], "c_custkey",150000);
	s.AddAtt(relName[0], "c_nationkey",25);

	s.AddRel(relName[1],1500000);
	s.AddAtt(relName[1], "o_orderkey",1500000);
	s.AddAtt(relName[1], "o_custkey",150000);
	
	s.AddRel(relName[2],6001215);
	s.AddAtt(relName[2], "l_orderkey",1500000);
	
	s.AddRel(relName[3],25);
	s.AddAtt(relName[3], "n_nationkey",25);
	
	char *cnf = "(c_custkey = o_custkey)  AND (o_orderdate > '1994-01-23') ";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName, 2);

	cnf = " (l_orderkey = o_orderkey) ";
	yy_scan_string(cnf);                                                                               	yyparse();

	s.Apply(final, relName, 3);  
	
	cnf = "(c_nationkey = n_nationkey) ";
	yy_scan_string(cnf);                                                                               	yyparse();	
	
	double result = s.Estimate(final, relName, 4);
	
	ASSERT_NEAR (2000405, result, 0.1);
	
	s.Apply(final, relName, 4);  
	
	s.Write(statFileName);

}

TEST_F(Project4Tests, TestingStatisticsTestcase11) {
	
	Statistics s;
    char *relName[] = { "part",  "lineitem"};

	s.Read(statFileName);
	
	s.AddRel(relName[0],200000);
	s.AddAtt(relName[0], "p_partkey",200000);
	s.AddAtt(relName[0], "p_container",40);

	s.AddRel(relName[1],6001215);
	s.AddAtt(relName[1], "l_partkey",200000);
	s.AddAtt(relName[1], "l_shipinstruct",4);
	s.AddAtt(relName[1], "l_shipmode",7);

	char *cnf = "(l_partkey = p_partkey) AND (l_shipmode = 'AIR' OR l_shipmode = 'AIR REG') AND (p_container ='SM BOX' OR p_container = 'SM PACK')  AND (l_shipinstruct = 'DELIVER IN PERSON')";

	yy_scan_string(cnf);
	yyparse();
	
	double result = s.Estimate(final, relName,2);
	
	ASSERT_NEAR (21432.9, result, 0.5);
	
	s.Apply(final, relName,2);
	
	s.Write(statFileName);
	
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}