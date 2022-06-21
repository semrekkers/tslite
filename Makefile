HEADERS = src/array.h
SOURCE  = src/array.c src/tslite.c

.PHONY: all
all:
	$(MAKE) -C src

.PHONY: clean
clean:
	rm -rf src/*.o src/*.so
