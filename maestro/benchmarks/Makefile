#

.PRECIOUS: *.log

PLOTFILES=plain.pdf

VIDEO_PLAYER_JAR = videoplayer-0.0.1.jar

DEPLOYFILES=das3.grid das3.script maestro.applications

%.experiment: build-experiment.py
	python build-experiment.py $* > $@

%.logs: %.experiment run-experiment $(DEPLOYFILES) $(VIDEO_PLAYER_JAR)
	echo "export TAG=$*" > settag-$*.sh
	rm -rf $*.logs
	./run-experiment $*.experiment
	rm -f settag-$*.sh

%.result: %.logs average-duration.py
	python average-duration.py $* $*.logs/out.* > $@

RANGE=2 4 8 12 16 20 32 40 52 64 72 80
PLAIN_POINTS=$(patsubst %,plain-%.result,1 $(RANGE))
ONETASK_POINTS=$(patsubst %,onetask-%.result,1 $(RANGE))
NO_POINTS=$(patsubst %,no-%.result,$(RANGE))
SLOW_POINTS=$(patsubst %,slow-%.result,$(RANGE))
LEARNPLAIN_POINTS=$(patsubst %,plain-%.learned-result,1 $(RANGE))
LEARNONETASK_POINTS=$(patsubst %,onetask-%.learned-result,1 $(RANGE))
LEARNNO_POINTS=$(patsubst %,no-%.learned-result,$(RANGE))
LEARNSLOW_POINTS=$(patsubst %,slow-%.learned-result,$(RANGE))


PRECIOUSJUNK=*.logs *.result $(PLOTFILES) plain.table one.table slow.table no.table
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

%.learned-result: %.result learn%.result subtract-results.py
	echo learn$*.result $*.result
	python subtract-results.py `cat learn$*.result` `cat $*.result` > $@

learnedplain.table: $(LEARNEDPLAIN_POINTS)
	cat $(LEARNEDPLAIN_POINTS) > $@

learnedonetask.table: $(LEARNEDONETASK_POINTS)
	cat $(LEARNEDONETASK_POINTS) > $@

learnedno.table: $(LEARNEDNO_POINTS)
	cat $(LEARNEDNO_POINTS) > $@

learnedslow.table: $(LEARNEDSLOW_POINTS)
	cat $(LEARNEDSLOW_POINTS) > $@

plain.pdf: plain.eps
	ps2pdf plain.eps > plain.pdf

plain.eps: slow.table learnedslow.table onetask.table learnedonetask.table no.table learnedno.table plain.table learnedplain.table plain-eps.gpl
	gnuplot plain-eps.gpl > $@

clean:
	rm -f $(JUNK)

empty:
	rm -f $(JUNK) $(PRECIOUSJUNK)

###
