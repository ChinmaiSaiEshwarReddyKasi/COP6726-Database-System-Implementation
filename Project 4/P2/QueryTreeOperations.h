#include "Statistics.h"
#include <map>
#include <string>
#include <vector>
#include <utility>
#include <cstring>
#include <fstream>
#include <iostream>

class QueryTreeOperations {
public:
    void GetJoinsAndSelects(vector <AndList> &joins, vector <AndList> &selects, vector <AndList> &joinDepSel,
                                   Statistics s);
    void GetTables(vector<string>& relations);
    QueryTreeOperations();
};
