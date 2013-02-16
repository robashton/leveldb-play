default: all

all: build run

run:
	nunit-console  Test.dll

build: 
	mono-csc -r:nunit.framework.dll -lib:./lib Test.cs /t:library
