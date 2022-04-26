#include "Statistics.h"
#include <map>
#include <string>
#include <vector>
#include <utility>
#include <cstring>
#include <fstream>
#include <iostream>

class NodeDataOperations {
public:
    void getOperationsData(vector <AndList> &joins, vector <AndList> &selects, vector <AndList> &joinDepSel, Statistics s);
    void getTableData(vector<string>& r);
    NodeDataOperations();
};
