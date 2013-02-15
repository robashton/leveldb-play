default: build

build:
	g++ -o ./bin/main ./src/main.cpp ./lib/libleveldb.so.1 -I ./include
	
