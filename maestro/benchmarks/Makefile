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
PLAIN_2000_POINTS=$(patsubst %,plain-%-2000.result,1 $(RANGE))
ONETASK_2000_POINTS=$(patsubst %,onetask-%-2000.result,1 $(RANGE))
NO_2000_POINTS=$(patsubst %,no-%-2000.result,$(RANGE))
SLOW_2000_POINTS=$(patsubst %,slow-%-2000.result,$(RANGE))
PLAIN_200_POINTS=$(patsubst %,plain-%-200.result,1 $(RANGE))
ONETASK_200_POINTS=$(patsubst %,onetask-%-200.result,1 $(RANGE))
NO_200_POINTS=$(patsubst %,no-%-200.result,$(RANGE))
SLOW_200_POINTS=$(patsubst %,slow-%-200.result,$(RANGE))
PLAIN_LEARNED_POINTS=$(patsubst %,plain-%.learned-result,1 $(RANGE))
ONETASK_LEARNED_POINTS=$(patsubst %,onetask-%.learned-result,1 $(RANGE))
NO_LEARNED_POINTS=$(patsubst %,no-%.learned-result,$(RANGE))
SLOW_LEARNED_POINTS=$(patsubst %,slow-%.learned-result,$(RANGE))

TERMINATION_FRACTIONS=0.0 0.2 0.4 0.6 0.8
TERMINATION_PLAIN_2000_POINTS=${patsubst %,termination-plain-%.result,$(TERMINATION_FRACTIONS)}
TERMINATION_SLOW_2000_POINTS=${patsubst %,termination-slow-%.result,$(TERMINATION_FRACTIONS)}

POINTS=$(PLAIN_2000_POINTS) $(ONETASK_2000_POINTS) $(NO_2000_POINTS) $(SLOW_2000_POINTS) $(PLAIN_200_POINTS) $(ONETASK_200_POINTS) $(NO_200_POINTS) $(SLOW_200_POINTS) $(TERMINATION_PLAIN_2000_POINTS) $(TERMINATION_SLOW_2000_POINTS)
LOGFILES=$(POINTS:.result=.logs)

.PRECIOUS: $(LOGFILES)


PRECIOUSJUNK=*.logs *.result $(PLOTFILES) plain.table one.table slow.table no.table
JUNK=*.experiment

all: $(PLOTFILES)

x:
	echo [$(LOGFILES)] 

plain.table: $(PLAIN_2000_POINTS)
	cat $(PLAIN_2000_POINTS) > $@

onetask.table: $(ONETASK_2000_POINTS)
	cat $(ONETASK_2000_POINTS) > $@

no.table: $(NO_2000_POINTS)
	cat $(NO_2000_POINTS) > $@

slow.table: $(SLOW_2000_POINTS)
	cat $(SLOW_2000_POINTS) > $@

termination-plain.table: $(TERMINATION_PLAIN_2000_POINTS)
	cat $(TERMINATION_PLAIN_2000_POINTS) > $@

termination-slow.table: $(TERMINATION_SLOW_2000_POINTS)
	cat $(TERMINATION_SLOW_2000_POINTS) > $@

%.learned-result: %-2000.result %-200.result
	python subtract-results.py `cat $*-200.result` `cat $*-2000.result` $@

learnedplain.table: $(PLAIN_LEARNED_POINTS)
	cat $(PLAIN_LEARNED_POINTS) > $@

learnedonetask.table: $(ONETASK_LEARNED_POINTS)
	cat $(ONETASK_LEARNED_POINTS) > $@

learnedno.table: $(NO_LEARNED_POINTS)
	cat $(NO_LEARNED_POINTS) > $@

learnedslow.table: $(SLOW_LEARNED_POINTS)
	cat $(SLOW_LEARNED_POINTS) > $@

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
