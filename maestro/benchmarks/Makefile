#

plain-%.experiment: build-experiment.py
	python build-experiment.py plain $* > $@


plain-%.result: plain-%.experiment run-experiment
	./run-experiment $<
