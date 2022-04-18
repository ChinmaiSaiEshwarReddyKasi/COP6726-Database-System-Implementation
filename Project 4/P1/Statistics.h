#ifndef STATISTICS_H
#define STATISTICS_H
#include <iostream>
#include <utility>
#include <fstream>
#include "ParseTree.h"
#include <vector>
#include <map>
#include <string>
#include <cstring>

using namespace std;
class AttsData;
class RelData;
typedef map<string, AttsData> AttrMap;
typedef map<string, RelData> RelMap;

class AttsData {
public:
    int differentTuples;
	string nameAtt;

	AttsData ();
    AttsData (string name, int num);
	AttsData (const AttsData &copyMe);

    AttsData &operator= (const AttsData &copyMe);
};

class RelData {
public:
    RelData (string name, int t);
	double numTuples;
	bool isRelationPresent (string _relName);
    AttrMap attrMap;


	map<string, string> JointRelation;
	RelData &operator= (const RelData &copyMe);
    RelData (const RelData &copyMe);
	RelData ();
    bool checkJoint;
	string relName;
};

class Statistics {
private:

    double ComparisonOpr (ComparisonOp *cOP, char *rName[], int join);
	double oprAnd (AndList *al, char *rName[], int join);
    int GatherRelationDataOpr (Operand *o, char *rName[], int join, RelData &rData);
    double oprOR (OrList *orOP, char *rName[], int join);

public:
	Statistics();
	Statistics(Statistics &copyMe);
    ~Statistics();
    Statistics operator= (Statistics &copyMe);


	RelMap relMap;
    void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attrName, int numDistincts);
    void CopyRel(char *oldName, char *newName);


    void Read(char *fromWhere);
    void Write(char *toWhere);


	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
    double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
    bool isRelInMap (string relName, RelData &relInfo);
};

#endif
