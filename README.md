# Very fastprep
> A fast data preprocessing for Swivel in Golang

There are 3 existing implementations of data pre-processing for a Swivel model:
 - Python
   https://github.com/tensorflow/models/blob/master/research/swivel/prep.py
 - C++
   https://github.com/tensorflow/models/blob/master/research/swivel/fastprep.cc
 - Scala (Apache Spark)
   https://github.com/bzz/swivel-spark-prep

This one takes advantage of Golang built-in CSP concurency primitives and scales well with a number of cores.

## Prepare test data
```
$ wget -c http://mattmahoney.net/dc/enwik9.zip -P data
$ unzip data/enwik9.zip -d data
$ wget https://raw.githubusercontent.com/facebookresearch/fastText/master/wikifil.pl
$ perl wikifil.pl data/enwik9.zip > data/text
```

## Build
```
go build ./cmd/veryfastprep
```

## Run
```
./veryfastprep -input data/text -n 4
```


### CLI

Available arguments
```
  -input string
      input text file
  -shard_size int
      matrix shard size (default 4096)
  -output_dir string
      a dir for all output data to be stored

  -n int
      number of parallel IO threads (default 1)
  -block int
      size of the IO buffer per thread, in Mb (default 10)
  -v	print verbose output

  -blockprofile string
      write block profile to the file
  -cpuprofile string
      write CPU profile to the file
  -mutexprofile string
      write mutex contention profile to the file
  -tracing
      write execution traces to the file
  ```

