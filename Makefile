.PHONY: all c clean test go go-test

all: c

c:
	$(MAKE) -C c

test:
	$(MAKE) -C c test

go:
	cd go && go build ./...

go-test:
	cd go && go test ./...

clean:
	$(MAKE) -C c clean
