#ifndef STATISTICS_H
#define STATISTICS_H
#include "ParseTree.h"
#include <map>
#include <string>
#include <vector>
#include <utility>
#include <cstring>
#include <fstream>
#include <iostream>

using namespace std;
class RelationInfo;
typedef map<string, RelationInfo> RelMap;
class AttributeInfo;
typedef map<string, AttributeInfo> AttrMap;

class AttributeInfo {
public:
    int distinctTuples;
	string attrName;

    AttributeInfo (string name, int num);
	AttributeInfo ();
    AttributeInfo &operator= (const AttributeInfo &copyMe);
	AttributeInfo (const AttributeInfo &copyMe);
};

class RelationInfo {
public:
    bool isJoint;
	double numTuples;
    AttrMap attrMap;
	string relName;
	map<string, string> relJoint;
    RelationInfo (const RelationInfo &copyMe);
	RelationInfo ();
	RelationInfo &operator= (const RelationInfo &copyMe);
	bool isRelationPresent (string _relName);
    RelationInfo (string name, int tuples);
};

class Statistics {
private:
    int GetRelForOp (Operand *operand, char *relName[], int numJoin, RelationInfo &relInfo);
    double ComOp (ComparisonOp *comOp, char *relName[], int numJoin);
	double AndOp (AndList *andList, char *relName[], int numJoin);
    double OrOp (OrList *orList, char *relName[], int numJoin);
public:
    ~Statistics();
	RelMap relMap;
    Statistics operator= (Statistics &copyMe);
	Statistics();
	Statistics(Statistics &copyMe);
    void Write(char *toWhere);
    void CopyRel(char *oldName, char *newName);
    void AddRel(char *relName, int numTuples);
    void Read(char *fromWhere);
    double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
    bool isRelInMap (string relName, RelationInfo &relInfo);
	void AddAtt(char *relName, char *attrName, int numDistincts);
	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
};

#endif
