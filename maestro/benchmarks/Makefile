#

PLOTFILES=plain.eps

VIDEO_PLAYER_JAR = videoplayer-0.0.1.jar

DEPLOYFILES=das3.grid das3.script maestro.applications

%.experiment: build-experiment.py
	python build-experiment.py $* > $@

%.result: %.experiment run-experiment $(DEPLOYFILES) $(VIDEO_PLAYER_JAR)
	rm -rf logs/$*-output
	./run-experiment $*.experiment
	python average-duration.py $* $*-output/out* > $*.result
	mv -f $*-output logs

RANGE=2 4 8 12 16 20 32 40 52 64 72 80
PLAIN_POINTS=$(patsubst %,plain-%.result,1 $(RANGE))
ONETASK_POINTS=$(patsubst %,onetask-%.result,1 $(RANGE))
NO_POINTS=$(patsubst %,no%.result,$(RANGE))
SLOW_POINTS=$(patsubst %,slow-%.result,$(RANGE))


PRECIOUSJUNK=*.result $(PLOTFILES) plain.table one.table slow.table no.table
JUNK=*.experiment

all: $(PLOTFILES)

x:
	echo [$(ONETASK_POINTS)] 

plain.table: $(PLAIN_POINTS)
	cat $(PLAIN_POINTS) > $@

onetask.table: $(ONETASK_POINTS)
	cat $(ONETASK_POINTS) > $@

no.table: $(NO_POINTS)
	cat $(NO_POINTS) > $@

slow.table: $(SLOW_POINTS)
	cat $(SLOW_POINTS) > $@

plain.pdf: plain.eps plain.table plain-one.table
	ps2pdf plain.eps > plain.pdf

plain.eps: plain.table onetask.table no.table slow.table plain-eps.gpl
	gnuplot plain-eps.gpl > $@

clean:
	rm -f $(JUNK)

empty:
	rm -f $(JUNK) $(PRECIOUSJUNK)

###
# Dependencies

plain.eps plain.pdf: plain.table plain-one.table
