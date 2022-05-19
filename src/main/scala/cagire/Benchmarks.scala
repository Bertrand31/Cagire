package cagire

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

object States {

  @State(Scope.Thread)
  class MyState {
    val virginCagire = Cagire.bootstrap()
    val hydratedCagire = Cagire.bootstrap().ingestFileHandler("t8.shakespeare.txt").get
  }
}

@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 3)
@Warmup(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 3)
@Fork(value = 2, jvmArgsAppend = Array())
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(value = 1)
class Benchmarks {

  @Benchmark
  def build(state: States.MyState, blackhole: Blackhole): Unit = {
    val built = Cagire.bootstrap()
    blackhole.consume(built)
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.SECONDS)
  def ingest(state: States.MyState, blackhole: Blackhole): Unit = {
    val hydrated = state.virginCagire.ingestFileHandler("t8.shakespeare.txt").get
    blackhole.consume(hydrated)
  }

  @Benchmark
  def lookup(state: States.MyState, blackhole: Blackhole): Unit = {
    val results = state.hydratedCagire.searchPrefix("partake")
    blackhole.consume(results)
  }
}
