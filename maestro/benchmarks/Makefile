#

PLOTFILES=plain.eps

VIDEO_PLAYER_JAR = videoplayer-0.0.1.jar

DEPLOYFILES=das3.grid das3.script maestro.applications

plain-one-%.experiment: build-plain-one-experiment.py
	python build-plain-one-experiment.py $* > $@

plain-%.experiment: build-plain-experiment.py
	python build-plain-experiment.py $* > $@

plain-one-%.result: plain-one-%.experiment run-experiment $(DEPLOYFILES) $(VIDEO_PLAYER_JAR)
	./run-experiment $<
	python average-duration.py $* plain-run$*-out.* > plain-one-$*.result
	mv -f plain-run$*-out.* logs

plain-%.result: plain-%.experiment run-experiment $(DEPLOYFILES) $(VIDEO_PLAYER_JAR)
	./run-experiment $<
	python average-duration.py $* plain-run$*-out.* > plain-$*.result
	mv -f plain-run$*-out.* logs

RANGE=1 2 4 8 12 16 20 32 40 52 64 72 80
PLAIN_POINTS=$(patsubst %,plain-%.result,$(RANGE))
PLAIN_ONE_POINTS=$(patsubst %,plain-one-%.result,$(RANGE))


PRECIOUSJUNK=plain-*.result $(PLOTFILES) plain.table plain-one.table
JUNK=plain-*.experiment

all: $(PLOTFILES)

x:
	echo [$(PLAIN_ONE_POINTS)] 

plain.table: $(PLAIN_POINTS)
	cat $(PLAIN_POINTS) > $@

plain-one.table: $(PLAIN_ONE_POINTS)
	cat $(PLAIN_ONE_POINTS) > $@

plain.pdf: plain.eps plain.table plain-one.table
	ps2pdf plain.eps > plain.pdf

plain.eps: plain.table plain-one.table plain-eps.gpl
	gnuplot plain-eps.gpl > $@

clean:
	rm -f $(JUNK)

empty:
	rm -f $(JUNK) $(PRECIOUSJUNK)

###
# Dependencies

plain.eps plain.pdf: plain.table plain-one.table
