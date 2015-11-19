# Rabbit MQ Stats DB Stress Test

Build with plain old

```shell
$ make
```

which will fetch the Erlang Rabbit MQ client dependency and compile
the stress test module.

Dialyze the code using

```shell
$ make dialyze
```

Run the stress test using the `run.sh` script, which takes two
arguments, the node name of the Rabbit MQ server you want to
stress test and the name of a CSV file, where results get written.
By default it assumes you will be running on the same machine as
the Rabbit MQ server. But it is possible to provide a machine name
as well:

```shell
$ ./run.sh rabbit results.csv
$ ./run.sh rabbit-test@My-MacBook-Pro /tmp/rmq-stats.csv
```
