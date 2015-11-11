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

Run the stress test using the `run.sh` script, which takes as a
single argument, the node name of the Rabbit MQ server you want to
stress test. By default it assumes you will be running on the same
machine. It is possible to provide a machine name as well:

```shell
$ ./run.sh rabbit
$ ./run.sh rabbit-test@My-MacBook-Pro
```
