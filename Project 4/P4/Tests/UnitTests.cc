#include <gtest/gtest.h>
#include <math.h>
#include <iostream>
#include <stdlib.h>
#include "../Statistics.h"

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

char *fName = "tempTest.txt";

TEST (StatsTest, writeTest) {
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
if(fabs(result - 800000) > 0.1) {
cout<< result << endl;
cout<<"error in estimating Q1 before apply \n ";
}
s.Apply(final, relName, 2);
s.Write(fName);
ifstream in (fName);
int numRel=0;
in >> numRel;
in.close();
ASSERT_EQ(1,numRel);
}

TEST (StatsTest, ReadTest) {
Statistics stat2;
stat2.Read(fName);
ASSERT_EQ(stat2.relMap.size(), 1);
}


int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}