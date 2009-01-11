#

#PLOTFILES=overhead.pdf fault-tolerance.pdf
PLOTFILES=disturbance.eps learn-plain.eps learn-slow.eps slow-20-2000-counts.eps plain-20-2000-counts.eps fault-tolerance.eps plain5-20-2000-counts.eps

VIDEO_PLAYER_JAR = videoplayer-0.0.1.jar

DEPLOYFILES=das3.grid das3.script maestro.applications ../lib/maestro-0.0.1.jar

TABLES=slow-2000.table no-2000.table onetask-2000.table plain-2000.table \
  learn-plain-4.table learn-plain-16.table learn-plain-64.table \
  learn-slow-4.table learn-slow-16.table learn-slow-64.table \
  termination-plain-2000.table termination-slow-2000.table

termination-%.experiment: build-termination-experiment.py
	python build-termination-experiment.py $* > $@

termination-%.logs: termination-%.experiment run-experiment $(DEPLOYFILES) $(VIDEO_PLAYER_JAR)
	echo "export TAG=termination-$*" > settag-termination-$*.sh
	python compute-termination-runtime.py $* >> settag-termination-$*.sh
	rm -rf termination-$*.logs
	./run-experiment termination-$*.experiment
	rm -f settag-termination-$*.sh

%.experiment: build-experiment.py
	python build-experiment.py $* > $@

%-fs0.logs: %.experiment run-experiment $(DEPLOYFILES) $(VIDEO_PLAYER_JAR)
	echo "export TAG=$*" > settag-$*.sh
	python compute-runtime.py $* >> settag-$*.sh
	rm -rf $*-fs?.logs
	./run-experiment $*.experiment
	rm -f settag-$*.sh

termination-%.result: termination-%.logs average-termination-duration.py
	python average-termination-duration.py $* $@ termination-$*-fs0.logs/out.*

%.counts: %-fs0.logs extract-task-counts.py
	python extract-task-counts.py $* $@ $*-fs0.logs/out.*

%.result: %-fs0.logs average-duration.py
	python average-duration.py $* $@ $*-fs0.logs/out.*

#LEARNRANGE=4000 3000 2000 1500 1000 500 350 200 150 100
LEARNRANGE=3000 2000 1500 1000 500 350 200 150 100
#RANGE=80 72 64 52 40 32 28 20 16 12 8 4 2
#RANGE=2 4 8 12 16 20 28 32 40 52 64 72 80
#RANGE=2 4 8 12
RANGE=2 4 6 8 12 16 20 28 32 40 52 64 72 80

PLAIN_2000_POINTS=$(patsubst %,plain-%-2000.result,1 $(RANGE))
ONETASK_2000_POINTS=$(patsubst %,onetask-%-2000.result,1 $(RANGE))
NO_2000_POINTS=$(patsubst %,no-%-2000.result,$(RANGE))
SLOW_2000_POINTS=$(patsubst %,slow-%-2000.result,$(RANGE))
ONESLOW_2000_POINTS=$(patsubst %,oneslow-%-2000.result,$(RANGE))
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
TERMINATION_COUNTS=0 5 11 17 23
TERMINATION_PLAIN_2000_POINTS=${patsubst %,termination-plain-%.result,$(TERMINATION_FRACTIONS)}
TERMINATION_SLOW_2000_POINTS=${patsubst %,termination-slow-%.result,$(TERMINATION_FRACTIONS)}

POINTS=$(PLAIN_2000_POINTS) $(ONETASK_2000_POINTS) $(NO_2000_POINTS) $(SLOW_2000_POINTS) $(ONESLOW_2000_POINTS) $(PLAIN_200_POINTS) $(ONETASK_200_POINTS) $(NO_200_POINTS) $(SLOW_200_POINTS) $(TERMINATION_PLAIN_2000_POINTS) $(TERMINATION_SLOW_2000_POINTS)
LOGFILES=$(POINTS:.result=.logs)

.PRECIOUS: $(LOGFILES)


PRECIOUSJUNK=*.logs $(PLOTFILES) plain-2000.table one.table slow-2000.table no-2000.table
JUNK=*.experiment *.result *.counts

all: $(PLOTFILES)

install: $(PLOTFILES)
	cp $(PLOTFILES) ~/hpdc/reeuwijk/documenten/maestro

tables: $(TABLES)

x:
	echo [$(LEARN_PLAIN_4_POINTS)] 

plain5-20-2000.counts: plain5-20-2000-fs0.logs
	python extract-task-counts.py plain5-20-2000 plain5-20-2000.counts plain5-20-2000-fs0.logs/out.* plain5-20-2000-fs1.logs/out.* plain5-20-2000-fs2.logs/out.* plain5-20-2000-fs3.logs/out.* plain5-20-2000-fs4.logs/out.*

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

oneslow-2000.table: $(ONESLOW_2000_POINTS)
	cat $(ONESLOW_2000_POINTS) > $@

slow-2000.table: $(SLOW_2000_POINTS)
	cat $(SLOW_2000_POINTS) > $@

termination-plain-2000.table: $(TERMINATION_PLAIN_2000_POINTS)
	cat $(TERMINATION_PLAIN_2000_POINTS) > $@

termination-slow-2000.table: $(TERMINATION_SLOW_2000_POINTS)
	cat $(TERMINATION_SLOW_2000_POINTS) > $@

%-counts.eps: %.counts counts-barchart-eps.gpl
	rm -rf $*-tmp
	mkdir $*-tmp
	cp $*.counts $*-tmp/data.counts
	cd $*-tmp && gnuplot ../counts-barchart-eps.gpl > ../$@
	rm -rf $*-tmp

%.pdf: %.eps
	ps2pdf $*.eps > $@

disturbance.eps: slow-2000.table no-2000.table disturbance-eps.gpl plain-2000.table oneslow-2000.table onetask-2000.table
	gnuplot disturbance-eps.gpl > $@

homogeneous.eps: onetask-2000.table plain-2000.table homogeneous-eps.gpl
	gnuplot homogeneous-eps.gpl > $@

learn-plain.eps: learn-plain-64.table learn-plain-16.table learn-plain-4.table  learn-plain-eps.gpl
	gnuplot learn-plain-eps.gpl > $@

learn-slow.eps: learn-slow-64.table learn-slow-16.table learn-slow-4.table  learn-slow-eps.gpl
	gnuplot learn-slow-eps.gpl > $@

termination-ideal.table: build-ideal-termination-table.py termination-plain-0.0.result
	python build-ideal-termination-table.py `cat termination-plain-0.0.result` $(TERMINATION_COUNTS) > $@

fault-tolerance.eps: termination-ideal.table termination-plain-2000.table termination-slow-2000.table fault-tolerance-eps.gpl
	gnuplot fault-tolerance-eps.gpl > $@

clean:
	rm -f $(JUNK)

empty:
	rm -f $(JUNK) $(PRECIOUSJUNK)

###
