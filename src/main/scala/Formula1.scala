import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Formula1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Formula 1").getOrCreate()
    val challenges = new Formula1Challenges(spark)
    challenges.challengeOne()
    challenges.challengeTwo()
    challenges.challengeThree()
    challenges.challengeFour()
    spark.stop()
  }
}

class Formula1Challenges(sparkSession: SparkSession) {
  private val spark = sparkSession

  private lazy val circuits = spark.read.format("csv").option("header", true).load("data/circuits.csv")
  private lazy val races = spark.read.format("csv").option("header", true).load("data/races.csv")
  private lazy val drivers = spark.read.format("csv").option("header", true).load("data/drivers.csv")
  private lazy val results = spark.read.format("csv").option("header", true).load("data/results.csv")
  private lazy val pitStops = spark.read.format("csv").option("header", true).load("data/pit_stops.csv")
  private lazy val lapTimes = spark.read.format("csv").option("header", true).load("data/lap_times.csv")

  def challengeOne(): Unit = {
    val racesInMonaco = races
      .join(circuits, "circuitId", "inner")
      .where(circuits("name") === "Circuit de Monaco")
    val monacoWinners = racesInMonaco
      .join(results, "raceId", "inner")
      .where(results("position") === 1)
    val monacoWinnersNames = monacoWinners
      .join(drivers, "driverId", "inner")
      .select(drivers("forename"), drivers("surname"), races("year"))
      .orderBy(desc("year"))

    monacoWinnersNames.write.format("csv").option("header", true).save("results/monaco-winners")
  }

  def challengeTwo(): Unit = {
    val fastestLapDriversSum = results
      .where(results("rank") === 1)
      .groupBy(results("driverId"))
      .agg(sum("rank") as "fastest_laps")
      .join(drivers, "driverId", "inner")
      .select("forename", "surname", "fastest_laps")
      .orderBy(desc("fastest_laps"))

    fastestLapDriversSum.write.format("csv").option("header", true).save("results/drivers-fastest-laps-sum")
  }

  def challengeThree(): Unit = {
    val racesIn2022 = races.filter(races("year") === 2022)
    val pitStopsRaces2022 = racesIn2022.join(pitStops, "raceId", "inner")
    val pitStopsPerDriver = pitStopsRaces2022
      .groupBy(races("name"), col("driverId"))
      .agg(max("stop") as "stops")
    val pitStopsAvgPerRace = pitStopsPerDriver
      .groupBy("name")
      .agg(avg("stops"))

    pitStopsAvgPerRace.write.format("csv").option("header", true).save("results/pit-stops-2022-avg")
  }

  def challengeFour(): Unit = {
    lapTimes.createOrReplaceTempView("lap_times")
    drivers.createOrReplaceTempView("drivers")

    def millisecondsToHours(n: Long): Long = {
      n / 3600000
    }

    val millisecondsToHoursUDF = udf(millisecondsToHours _)
    spark.udf.register("msToHours", millisecondsToHoursUDF)

    val driversTime = spark.sql(
      """
        |SELECT forename, surname, msToHours(sum(milliseconds)) AS time
        |FROM lap_times
        |JOIN drivers ON lap_times.driverId = drivers.driverId
        |GROUP BY drivers.forename, drivers.surname
        |ORDER BY time DESC
        |""".stripMargin)

    driversTime.write.format("csv").option("header", true).save("results/drivers-time")
  }
}
      
