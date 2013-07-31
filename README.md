# RiakStorer

A [UDF StoreFunc](http://pig.apache.org/docs/r0.8.0/udf.html#Store+Functions) for [Apache Pig](http://pig.apache.org/) designed to bulk-load data into [Riak](http://basho.com/riak/). Inspired by [pig-redis](https://github.com/mattb/pig-redis) store function.

## Please note: The loader uses the REST APIs, therefore it is increbibly slow for large datasets. 
To improve performances (not included in this release):
  - Use ProtBuf Apis
  - Batch writes

## Compiling and running

Compile:

Dependencies are automatically retrieved using [gradle](http://www.gradle.org/). Gradle itself is automatically retrieved using the gradle wrapper.

    $ ./gradlew jar

Use:

    $ pig
    grunt> REGISTER build/libs/pig-riak.jar;
    grunt> a = LOAD 'somefile.tsv' USING PigStorage('\t');
    grunt> STORE a INTO 'bucket-name' USING com.brunobonacci.pig.riak.RiakStorer('text/plain', 'localhost');

The build process produces a jar with all necessary dependencies.

## Loading strategy

RiakStorer uses the HTTP/Rest interface of Riak to load data.
It expects a two column table where the first column is the **key** and the second column is the **content** to be uploaded.
The Content-Type can be defined on the RiakStorer constructor, together with the destination host and port.
The destination **bucket** must be set as destination location for the STORE function.

If the table contains more than two columns they are simply ignored. Use additional steps and UDF function to transform your table into the desired string content before the final upload.

If the key upload fails an Exception is raised.

## License

Distributed under [Apache Lincense 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

