default: all

build:
	g++ -o ./bin/main ./src/main.cpp ./lib/libleveldb.so.1 -I ./include
	
run:
	./bin/main

all: build run
