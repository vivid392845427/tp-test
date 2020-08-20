all: bin/tgen bin/xrun

bin/tgen: FORCE

bin/xrun: FORCE

bin/%:
	mkdir -p bin
	go build -o $@ ./tools/$*

FORCE:
