# Run batch experiments

This tool takes config in the YAML-format like [experiment.yaml](./experiment.yaml) and runs a simulation for every possible combination of parameters from config. The config contains:
* List of simulation plans
* List of system configurations
* List of replication strategies
* List of placement strategies

To run the tool with the provided [config](./experiment.yaml) first generate `experiment_plan_1` and `experiment_plan_2` using [alibaba_parser](../alibaba_parser).
