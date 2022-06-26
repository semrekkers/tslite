HEADERS  = tslite.h array_buffer.h
INTERMED = array_each.c
SOURCE   = array.c tslite.c
OBJECTS	 = array.o tslite.o

tslite.so: $(OBJECTS)
	gcc -g -fPIC -Wall -Wextra -shared -o tslite.so $(OBJECTS)

%.o: %.c $(HEADERS) $(INTERMED)
	gcc -c -g -fPIC -Wall -Wextra -o $@ $<
