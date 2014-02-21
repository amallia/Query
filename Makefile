STD=-std=c++0x -D CPP0X
DBG=$(STD) -g3 -Wall -D DEBUG
RLS=$(STD) -O3 -D NDEBUG

PROF=$(STD) -O3 -pg -D GPROFILING -D NDEBUG 
  
TYPE=$(PROF)
TYPE=$(DBG)
TYPE=$(RLS)

all: main.o pfor.o ListIterator.o profiling.o main.cpp DocidOriented_BMW.o PostingOriented_BMW.o TrecReader.o exhaustiveOR.o sql/sqlite3.o
	g++ $(TYPE) main.o TrecReader.o pfor.o ListIterator.o profiling.o DocidOriented_BMW.o PostingOriented_BMW.o exhaustiveOR.o sql/sqlite3.o -o main -lpthread
# -ldl 
clean:
	rm *.o main

CPPC=g++ $(TYPE) -c   

main.o: main.cpp TrecReader.h globals.h Makefile
	$(CPPC) main.cpp

PostingOriented_BMW.o: PostingOriented_BMW.h PostingOriented_BMW.cpp ListIterator.h globals.h Makefile
	$(CPPC) PostingOriented_BMW.cpp

DocidOriented_BMW.o: DocidOriented_BMW.h DocidOriented_BMW.cpp ListIterator.h globals.h BlockGens.h Makefile
	$(CPPC) DocidOriented_BMW.cpp

pfor.o: pfor.h pfor.cpp globals.h Makefile
	$(CPPC) pfor.cpp  
	
ListIterator.o: ListIterator.cpp ListIterator.h globals.h Makefile
	$(CPPC) ListIterator.cpp
	
TrecReader.o: TrecReader.cpp TrecReader.h globals.h BlockGens.h Makefile
	$(CPPC) TrecReader.cpp 

profiling.o: profiling.cpp 	profiling.h globals.h Makefile
	$(CPPC) profiling.cpp

exhaustiveOR.o: exhaustiveOR.h exhaustiveOR.cpp ListIterator.h globals.h utils.h Makefile
	$(CPPC) exhaustiveOR.cpp	

sql/sqlite3.o: 
	gcc -c -O3 -DSQLITE_THREADSAFE=2 -DSQLITE_OMIT_LOAD_EXTENSION=1 sql/sqlite3.c -o sql/sqlite3.o
	   

TIMING=$(STD) -O3 -D TIMING 
TYPE=$(TIMING)