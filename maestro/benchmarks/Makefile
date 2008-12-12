#

PLOTFILES=speedup.eps

plain-%.experiment: build-experiment.py
	python build-experiment.py plain $* > $@

plain-%.log plain-%.result: plain-%.experiment run-experiment
	./run-experiment $<

RANGE=1 2 4 8 16
PLAIN_POINTS=$(patsubst %,plain%.txt,$(RANGE))


PRECIOUSJUNK=plain-*.result $(PLOTFILES)
JUNK=plain-*.experiment

all: $(PLOTFILES)

x:
	echo [$(RANGE)] 
	echo [$(patsubst %,XX%,$(RANGE))] 

plain.table: $(PLAIN_POINTS)
	cat $(PLAIN_POINTS) > $@

plain%.txt:
	echo "$* 1$*" > $@

%.eps: %.gpl
	gnuplot $*.gpl > $@

clean:
	rm -f $(JUNK)

empty:
	rm -f $(JUNK) $(PRECIOUSJUNK)

###
# Dependencies

speedup.eps: plain.table
