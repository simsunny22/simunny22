cc = gcc
objects = btree.o queue.o

btree : ${objects}
	$(cc) ${objects} -o btree

btree.o : btree.c btree.h
	$(cc) -c btree.c

queue.o : queue.c queue.h
	$(cc) -c queue.c

clean:
	rm *.o btree
