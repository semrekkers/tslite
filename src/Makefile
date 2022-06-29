HEADERS  = tslite.h array_buffer.h
INTERMED = array_each.c
SOURCE   = array.c tslite.c
OBJECTS	 = array.o tslite.o
CFLAGS	 = -O2 -fPIC -Wall -Wextra

tslite.so: $(OBJECTS)
	gcc $(CFLAGS) -shared -o tslite.so $(OBJECTS)

%.o: %.c $(HEADERS) $(INTERMED)
	gcc -c $(CFLAGS) -o $@ $<

.PHONY: debug
debug: CFLAGS = -g -fPIC -Wall -Wextra
debug: tslite.so
