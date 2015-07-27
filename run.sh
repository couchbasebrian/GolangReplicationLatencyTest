#!/bin/bash
# Run go program
# Set your GOPATH here
GOPATH=~/go-lang
export GOPATH
echo GOPATH is $GOPATH
echo
go run GolangReplicationLatencyTest.go
