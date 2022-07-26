HEADERS	= tslite.h
SOURCE	= tslite.c
CFLAGS	= -O2 -fPIC -Wall -Wextra

tslite.so: $(HEADERS) $(SOURCE)
	gcc $(CFLAGS) -shared -o $@ $(SOURCE)

.PHONY: debug
debug: CFLAGS = -g -fPIC -Wall -Wextra
debug: tslite.so

.PHONY: clean
clean:
	rm -vf *.o *.so
