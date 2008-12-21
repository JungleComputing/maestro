#

#PLOTFILES=overhead.pdf fault-tolerance.pdf
PLOTFILES=homogeneous.pdf disturbance.pdf

VIDEO_PLAYER_JAR = videoplayer-0.0.1.jar

DEPLOYFILES=das3.grid das3.script maestro.applications

termination-%.experiment:
	python build-termination-experiment.py termination-$* > $@

%.experiment: build-experiment.py
	python build-experiment.py $* > $@

%.logs: %.experiment run-experiment $(DEPLOYFILES) $(VIDEO_PLAYER_JAR)
	echo "export TAG=$*" > settag-$*.sh
	rm -rf $*.logs
	./run-experiment $*.experiment
	rm -f settag-$*.sh

%.result: %.logs
	python average-duration.py $* $@ $*.logs/out.*

#RANGE=2 4 8 12 16 20 32 40 52 64 72 80
RANGE=2 4 8 12 16 20 32 40 52 64 72 80
PLAIN_POINTS=$(patsubst %,plain-%.result,1 $(RANGE))
ONETASK_POINTS=$(patsubst %,onetask-%.result,1 $(RANGE))
NO_POINTS=$(patsubst %,no-%.result,$(RANGE))
SLOW_POINTS=$(patsubst %,slow-%.result,$(RANGE))
LEARNPLAIN_POINTS=$(patsubst %,learnplain-%.result,1 $(RANGE))
LEARNONETASK_POINTS=$(patsubst %,learnonetask-%.result,1 $(RANGE))
LEARNNO_POINTS=$(patsubst %,learnno-%.result,$(RANGE))
LEARNSLOW_POINTS=$(patsubst %,learnslow-%.result,$(RANGE))
LEARNEDPLAIN_POINTS=$(patsubst %,plain-%.learned-result,1 $(RANGE))
LEARNEDONETASK_POINTS=$(patsubst %,onetask-%.learned-result,1 $(RANGE))
LEARNEDNO_POINTS=$(patsubst %,no-%.learned-result,$(RANGE))
LEARNEDSLOW_POINTS=$(patsubst %,slow-%.learned-result,$(RANGE))

TERMINATION_FRACTIONS=0.0 0.2 0.4 0.6 0.8
TERMINATION_PLAIN_POINTS=${patsubst %,termination-plain-%.result,$(TERMINATION_FRACTIONS)}
TERMINATION_SLOW_POINTS=${patsubst %,termination-slow-%.result,$(TERMINATION_FRACTIONS)}

POINTS=$(PLAIN_POINTS) $(ONETASK_POINTS) $(NO_POINTS) $(SLOW_POINTS) $(LEARNPLAIN_POINTS) $(LEARNONETASK_POINTS) $(LEARNNO_POINTS) $(LEARNSLOW_POINTS) $(TERMINATION_PLAIN_POINTS) $(TERMINATION_SLOW_POINTS)
LOGFILES=$(POINTS:.result=.logs)

.PRECIOUS: $(LOGFILES)


PRECIOUSJUNK=*.logs *.result $(PLOTFILES) plain.table one.table slow.table no.table
JUNK=*.experiment

all: $(PLOTFILES)

x:
	echo [$(LOGFILES)] 

plain.table: $(PLAIN_POINTS)
	cat $(PLAIN_POINTS) > $@

onetask.table: $(ONETASK_POINTS)
	cat $(ONETASK_POINTS) > $@

no.table: $(NO_POINTS)
	cat $(NO_POINTS) > $@

slow.table: $(SLOW_POINTS)
	cat $(SLOW_POINTS) > $@

termination-plain.table: $(TERMINATION_PLAIN_POINTS)
	cat $(TERMINATION_PLAIN_POINTS) > $@

termination-slow.table: $(TERMINATION_SLOW_POINTS)
	cat $(TERMINATION_SLOW_POINTS) > $@

%.learned-result: %.result learn%.result
	echo learn$*.result $*.result
	python subtract-results.py `cat learn$*.result` `cat $*.result` $@

learnedplain.table: $(LEARNEDPLAIN_POINTS)
	cat $(LEARNEDPLAIN_POINTS) > $@

learnedonetask.table: $(LEARNEDONETASK_POINTS)
	cat $(LEARNEDONETASK_POINTS) > $@

learnedno.table: $(LEARNEDNO_POINTS)
	cat $(LEARNEDNO_POINTS) > $@

learnedslow.table: $(LEARNEDSLOW_POINTS)
	cat $(LEARNEDSLOW_POINTS) > $@

%.pdf: %.eps
	ps2pdf $*.eps > $@

disturbance.eps: slow.table learnedslow.table no.table learnedno.table  disturbance-eps.gpl
	gnuplot disturbance-eps.gpl > $@

homogeneous.eps: onetask.table learnedonetask.table plain.table learnedplain.table homogeneous-eps.gpl
	gnuplot homogeneous-eps.gpl > $@

fault-tolerance.pdf: fault-tolerance.eps
	ps2pdf fault-tolerance.eps > fault-tolerance.pdf

fault-tolerance.eps: termination-plain.table termination-slow.table fault-tolerance-eps.gpl
	gnuplot fault-tolerance-eps.gpl > $@

clean:
	rm -f $(JUNK)

empty:
	rm -f $(JUNK) $(PRECIOUSJUNK)

###
