#run scriptsrc/main/run-lab1-master.sh, start master
# build so
go build -race  -buildmode=plugin ../mrapps/wc.go

# run coordinator
rm mr-out*
rm mr-tmp*
go run -race mrcoordinator.go pg-*.txt