ReadMe  of  HW 2

How to compile the source code:
mix-compilation using gcc and g++.

1.Build index part:

parser-revised-again.o: parser-revised-again.cpp	
g++ -c parser-revised-again.cpp
trim.o :trim.cpp
	g++ -c trim.cpp
indexer.o : indexer.cpp
	g++ -c indexer.cpp 
./run : parser.o trim.o index.o
	g++ -o indexer parser.o trim.o indexer.o –lz
	

2. I/O efficient algorithm (external sorting):

./merge : merge.c
	gcc –o merge merge.c
usage:
	./merge 12  100000000  83 inputlist result outputlist

12 is length of struct positng.    //<WordID,DocID,TF>
100000000 means about 100MB memory.
83 means 83 files totally.
result means outputfile prefix.
outputlist       // if you wanna implement N passes external sorting, you need to save it as second round’s 
        // inputfile.
 




 



