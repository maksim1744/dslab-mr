```
parsed_1: -s 100000 -e 100020 -t ..\..\alibaba -o ..\..\alibaba\parsed_1 --min-tasks-in-job 1 --initial-data-size-from 10 --initial-data-size-to 20 --flops-per-byte-from 20 --flops-per-byte-to 50 --cpu-multiplier 16 --mem-multiplier 12800000 --instances-multiplier-from 5 --instances-multiplier-to 10

parsed_2: -s 200000 -e 200020 -t ..\..\alibaba -o ..\..\alibaba\parsed_2 --min-tasks-in-job 1 --initial-data-size-from 20 --initial-data-size-to 40 --flops-per-byte-from 50 --flops-per-byte-to 100 --cpu-multiplier 16 --mem-multiplier 12800000 --upload-job-inputs-in-advance
```
