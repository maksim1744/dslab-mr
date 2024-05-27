# Benchmark

For this benchmark the tool [run_experiment](../../tools/run_experiment/) can be used with the following config:

```yaml
plans:
- plan_path: ../../alibaba/benchmark_plan/plan.yaml
  dags_path: ../../alibaba/benchmark_plan/dags
systems:
- ../../examples/benchmark/system.yaml
replication_strategies:
- Random[replication_factor=3,chunk_distribution=ProhibitSameRack]
placement_strategies:
- Random
```

Before running it first generate `benchmark_plan` using [alibaba_parser](../../tools/alibaba_parser).
