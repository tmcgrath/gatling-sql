# gatling-sql

Extension for Gatling to stress your SQL database or Spark Thrift Server via JDBC

## Quick start

Run the Maven `test` task which will attempt to download dependencies,
compile and execute the `ThriftServerSimulation`

For more info, see [Spark Thrift Server Load Testing tutorial](https://supergloo.com/spark/apache-spark-thrift-server-load-testing-example/ "Spark Thrift Server Load Testing Example")

### References, Influences and Thanks

Original inspiration, simulation sample and pom.xml file from https://github.com/senkadam/gatlingsql
which is released with an Apache 2.0 License
https://github.com/senkadam/gatlingsql/blob/master/LICENSE


Overall the approach and design relies on the approach found in GatlingCql extension

https://github.com/gatling-cql/GatlingCql is released with an MIT License
https://github.com/gatling-cql/GatlingCql/blob/master/LICENSE

