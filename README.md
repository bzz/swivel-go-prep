# Very fastprep
> A fast data preprocessing for Swivel in Golangs

There are 3 implementation of data pre-processing required to train a Swivel models:
 - Python
   https://github.com/tensorflow/models/blob/master/research/swivel/prep.py
 - C++
   https://github.com/tensorflow/models/blob/master/research/swivel/fastprep.cc
 - Scala (Apache Spark)
   https://github.com/bzz/swivel-spark-prep

This one takes advantage of Golang built-in CSP concurency primitives.

## Prepare test data
```
$ mkdir data
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


