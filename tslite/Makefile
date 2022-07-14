HEADERS	= tslite.h
SOURCE	= tslite.c
OBJECTS	= tslite.o
CFLAGS	= -O2 -fPIC -Wall -Wextra

tslite.so: $(OBJECTS)
	gcc $(CFLAGS) -shared -o $@ $(OBJECTS)

%.o: %.c $(HEADERS)
	gcc -c $(CFLAGS) -o $@ $<

.PHONY: debug
debug: CFLAGS = -g -fPIC -Wall -Wextra
debug: tslite.so

.PHONY: static
static: CFLAGS := $(CFLAGS) -DSQLITE_CORE
static: tslite.so

.PHONY: clean
clean:
	rm -vf tslite.so $(OBJECTS)
