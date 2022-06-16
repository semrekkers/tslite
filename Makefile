HEADERS = 
SOURCE  = src/array.c src/utils.c src/tslite.c

.PHONY: all
all: tslite.so

tslite.so: $(SOURCE)
	gcc -g -fPIC -O2 -Wall -Wextra -shared $(SOURCE) -o tslite.so

.PHONY: clean
clean:
	rm -rf src/*.o tslite.so
