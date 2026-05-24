.PHONY: run-test run-prod build

build:
	go build -o bin/control .

run-test:
	APP_ENV=test go run .

run-prod:
	APP_ENV=prod go run .
