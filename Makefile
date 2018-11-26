.PHONY: clean all

all: reagent

sources = \
	sha256.c \
	minilang.c \
	ml_file.c \
	stringmap.c \
	linenoise.c \
	ra_events.c \
	ra_schema.c \
	reagent.c

CFLAGS += -std=gnu99 -I. -Igc/include -g -pthread -DGC_THREADS -D_GNU_SOURCE -DGC_DEBUG
LDFLAGS += -lm -ldl -g -lgc

reagent: Makefile $(sources) *.h
	gcc $(CFLAGS) $(sources) $(LDFLAGS) -o$@

clean:
	rm reagent
