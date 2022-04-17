CC = g++ -O2 -Wno-deprecated
tag = -i
test_out_tag = -ll

ifdef linux
tag = -n
test_out_tag = -ll
endif

INC_DIR = lib/googletest-master/googletest/include/
SRC_DIR = lib/googletest-master/googletest/
#DEPS = gtest.h
CFLAGS = -I$(INC_DIR)
SFLAGS = -I$(SRC_DIR)
CXXFLAGS=-std=c++11

a4-1.out: Statistics.o y.tab.o yyfunc.tab.o lex.yyfunc.o lex.yy.o test.o
	$(CC) -o a4-1.out Statistics.o y.tab.o yyfunc.tab.o lex.yyfunc.o lex.yy.o test.o $(test_out_tag)

a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o $(test_out_tag)

gtesting: Statistics.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o BigQ.o Pipe.o y.tab.o lex.yy.o gtest-all.o testing.o
	$(CC) -o gtesting Statistics.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o BigQ.o Pipe.o y.tab.o lex.yy.o gtest-all.o testing.o -ll

gtest-all.o: gtest/gtest-all.cc
	$(CC) -g -c gtest/gtest-all.cc

testing.o: Tests/UnitTests.cc
	$(CC) -g -c Tests/UnitTests.cc  -o testing.o

test.o: test.cc
	$(CC) -g -c test.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

a2-1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-1test.o
	$(CC) -o a2-1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-1-test.o $(test_out_tag)

a2-2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-2test.o
	$(CC) -o a2-2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-test.o $(test_out_tag)


Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" yyfunc.tab.c
# 	sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

a2-2test.o: a2-2-test.cc
	$(CC) -g -c a2-2-test.cc

a2-1test.o: a2-1-test.cc
	$(CC) -g -c a2-1-test.cc

clean:
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h