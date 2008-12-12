#

PLOTFILES=speedup.eps

DEPLOYFILES=das3.grid das3.script maestro.applications

plain-%.experiment: build-experiment.py
	python build-experiment.py plain $* > $@

plain-%.result: plain-%.experiment run-experiment $(DEPLOYFILES)
	./run-experiment $<
	python average-duration.py $* plain-run$*-out.* > plain-$*.result
	mv -f plain-run$*-out.* logs

RANGE=1 2 4 8 16
PLAIN_POINTS=$(patsubst %,plain-%.result,$(RANGE))


PRECIOUSJUNK=plain-*.result $(PLOTFILES)
JUNK=plain-*.experiment

all: $(PLOTFILES)

x:
	echo [$(RANGE)] 
	echo [$(PLAIN_POINTS)] 

plain.table: $(PLAIN_POINTS)
	cat $(PLAIN_POINTS) > $@

%.eps: %.gpl
	gnuplot $*.gpl > $@

clean:
	rm -f $(JUNK)

empty:
	rm -f $(JUNK) $(PRECIOUSJUNK)

###
# Dependencies

speedup.eps: plain.table
