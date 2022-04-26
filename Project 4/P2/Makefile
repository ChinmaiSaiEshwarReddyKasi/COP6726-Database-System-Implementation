
CC = g++ -O2 -Wno-deprecated

tag = -i
gTest_tag = -std=c++11 -lgtest -lgtest_main -pthread

ifdef linux
tag = -n
endif

a42.out: y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o SortedDBFile.o HeapDBFile.o NodeDataQuery.o Statistics.o NodeDataOperations.o main.o
	$(CC) -o a42.out y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o SortedDBFile.o HeapDBFile.o NodeDataQuery.o Statistics.o NodeDataOperations.o main.o -ll -lpthread

gTests: y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o SortedDBFile.o HeapDBFile.o NodeDataQuery.o Statistics.o NodeDataOperations.o gTests.o
	$(CC) -o gTests.out y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o SortedDBFile.o HeapDBFile.o NodeDataQuery.o Statistics.o NodeDataOperations.o gTests.o $(gTest_tag)

gTests.o: gTests.cc
	$(CC) -g -c gTests.cc $(gTest_tag)

NodeDataQuery.o: NodeDataQuery.cc
	$(CC) -g -c NodeDataQuery.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

main.o : main.cc
	$(CC) -g -c main.cc

NodeDataOperations.o : NodeDataOperations.cc
	$(CC) -g -c NodeDataOperations.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean:
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h