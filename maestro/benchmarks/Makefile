#

#PLOTFILES=overhead.pdf fault-tolerance.pdf
PLOTFILES=homogeneous.pdf disturbance.pdf learn-plain.pdf learn-slow.pdf fault-tolerance.pdf

VIDEO_PLAYER_JAR = videoplayer-0.0.1.jar

DEPLOYFILES=das3.grid das3.script maestro.applications ../lib/maestro-0.0.1.jar

TABLES=slow-2000.table no-2000.table onetask-2000.table plain-2000.table learn-plain-4.table learn-plain-16.table learn-plain-64.table learn-slow-4.table learn-slow-16.table learn-slow-64.table

termination-%.experiment: build-termination-experiment.py
	python build-termination-experiment.py termination-$* > $@

%.experiment: build-experiment.py
	python build-experiment.py $* > $@

%.logs: %.experiment run-experiment $(DEPLOYFILES) $(VIDEO_PLAYER_JAR)
	echo "export TAG=$*" > settag-$*.sh
	python compute-runtime.py $* >> settag-$*.sh
	rm -rf $*.logs
	./run-experiment $*.experiment
	rm -f settag-$*.sh

%.result: %.logs
	python average-duration.py $* $@ $*.logs/out.*

LEARNRANGE=4000 3000 2000 1500 1000 500 200 100
#RANGE=80 72 64 52 40 32 28 20 16 12 8 4 2
RANGE=2 4 8 12 16 20 28 32 40 52 64 72 80

PLAIN_2000_POINTS=$(patsubst %,plain-%-2000.result,1 $(RANGE))
ONETASK_2000_POINTS=$(patsubst %,onetask-%-2000.result,1 $(RANGE))
NO_2000_POINTS=$(patsubst %,no-%-2000.result,$(RANGE))
SLOW_2000_POINTS=$(patsubst %,slow-%-2000.result,$(RANGE))
PLAIN_200_POINTS=$(patsubst %,plain-%-200.result,1 $(RANGE))
ONETASK_200_POINTS=$(patsubst %,onetask-%-200.result,1 $(RANGE))
NO_200_POINTS=$(patsubst %,no-%-200.result,$(RANGE))
SLOW_200_POINTS=$(patsubst %,slow-%-200.result,$(RANGE))

LEARN_PLAIN_4_POINTS=$(patsubst %,plain-4-%.result, $(LEARNRANGE) )
LEARN_PLAIN_16_POINTS=$(patsubst %,plain-16-%.result, $(LEARNRANGE) )
LEARN_PLAIN_64_POINTS=$(patsubst %,plain-64-%.result, $(LEARNRANGE) )

LEARN_SLOW_4_POINTS=$(patsubst %,slow-4-%.result, $(LEARNRANGE) )
LEARN_SLOW_16_POINTS=$(patsubst %,slow-16-%.result, $(LEARNRANGE) )
LEARN_SLOW_64_POINTS=$(patsubst %,slow-64-%.result, $(LEARNRANGE) )

TERMINATION_FRACTIONS=0.0 0.2 0.4 0.6 0.8
TERMINATION_PLAIN_2000_POINTS=${patsubst %,termination-plain-%.result,$(TERMINATION_FRACTIONS)}
TERMINATION_SLOW_2000_POINTS=${patsubst %,termination-slow-%.result,$(TERMINATION_FRACTIONS)}

POINTS=$(PLAIN_2000_POINTS) $(ONETASK_2000_POINTS) $(NO_2000_POINTS) $(SLOW_2000_POINTS) $(PLAIN_200_POINTS) $(ONETASK_200_POINTS) $(NO_200_POINTS) $(SLOW_200_POINTS) $(TERMINATION_PLAIN_2000_POINTS) $(TERMINATION_SLOW_2000_POINTS)
LOGFILES=$(POINTS:.result=.logs)

.PRECIOUS: $(LOGFILES)


PRECIOUSJUNK=*.logs *.result $(PLOTFILES) plain-2000.table one.table slow-2000.table no-2000.table
JUNK=*.experiment

all: $(PLOTFILES)

tables: $(TABLES)

x:
	echo [$(LEARN_PLAIN_4_POINTS)] 

learn-plain-4.table: $(LEARN_PLAIN_4_POINTS)
	cat $(LEARN_PLAIN_4_POINTS) > $@

learn-plain-16.table: $(LEARN_PLAIN_16_POINTS)
	cat $(LEARN_PLAIN_16_POINTS) > $@

learn-plain-64.table: $(LEARN_PLAIN_64_POINTS)
	cat $(LEARN_PLAIN_64_POINTS) > $@

learn-slow-4.table: $(LEARN_SLOW_4_POINTS)
	cat $(LEARN_SLOW_4_POINTS) > $@

learn-slow-16.table: $(LEARN_SLOW_16_POINTS)
	cat $(LEARN_SLOW_16_POINTS) > $@

learn-slow-64.table: $(LEARN_SLOW_64_POINTS)
	cat $(LEARN_SLOW_64_POINTS) > $@

plain-2000.table: $(PLAIN_2000_POINTS)
	cat $(PLAIN_2000_POINTS) > $@

onetask-2000.table: $(ONETASK_2000_POINTS)
	cat $(ONETASK_2000_POINTS) > $@

no-2000.table: $(NO_2000_POINTS)
	cat $(NO_2000_POINTS) > $@

slow-2000.table: $(SLOW_2000_POINTS)
	cat $(SLOW_2000_POINTS) > $@

termination-plain-2000.table: $(TERMINATION_PLAIN_2000_POINTS)
	cat $(TERMINATION_PLAIN_2000_POINTS) > $@

termination-slow-2000.table: $(TERMINATION_SLOW_2000_POINTS)
	cat $(TERMINATION_SLOW_2000_POINTS) > $@

%.pdf: %.eps
	ps2pdf $*.eps > $@

disturbance.eps: slow-2000.table no-2000.table disturbance-eps.gpl
	gnuplot disturbance-eps.gpl > $@

homogeneous.eps: onetask-2000.table plain-2000.table homogeneous-eps.gpl
	gnuplot homogeneous-eps.gpl > $@

learn-plain.eps: learn-plain-4.table learn-plain-16.table learn-plain-64.table  learn-plain-eps.gpl
	gnuplot learn-plain-eps.gpl > $@

learn-slow.eps: learn-slow-4.table learn-slow-16.table learn-slow-64.table  learn-slow-eps.gpl
	gnuplot learn-slow-eps.gpl > $@

fault-tolerance.eps: termination-plain-2000.table termination-slow-2000.table fault-tolerance-eps.gpl
	gnuplot fault-tolerance-eps.gpl > $@

clean:
	rm -f $(JUNK)

empty:
	rm -f $(JUNK) $(PRECIOUSJUNK)

###
