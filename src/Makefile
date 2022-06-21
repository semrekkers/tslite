HEADERS = tslite.h array_buffer.h
SOURCE  = array.c tslite.c
OBJECTS	= array.o tslite.o

tslite.so: $(OBJECTS)
	gcc -g -fPIC -Wall -Wextra -shared -o tslite.so $(OBJECTS)

%.o: %.c $(HEADERS)
	gcc -c -g -fPIC -Wall -Wextra -o $@ $<
