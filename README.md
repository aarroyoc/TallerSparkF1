# TallerSparkF1
Taller de Spark con datos de la Formula 1

```sh
$ sbt package
$ SPARK_FOLDER/bin/spark-submit --class "Formula1" --master local[4] target/scala-2.12/formula-1-spark_2.12-1.0.jar
```