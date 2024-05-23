```
parsed_2: -s 200000 -e 200020 -t ..\..\alibaba -o ..\..\alibaba\parsed_2 --min-tasks-in-job 1 --initial-data-size-from 200 --initial-data-size-to 400 --flops-per-byte-from 0.2 --flops-per-byte-to 0.5 --cpu-multiplier 8 --mem-multiplier 12800000 --upload-job-inputs-in-advance

parsed_3: -s 200100 -e 200200 -t ..\..\alibaba -o ..\..\alibaba\parsed_3 --min-tasks-in-job 1 --initial-data-size-from 0.5 --initial-data-size-to 1 --flops-per-byte-from 0.05 --flops-per-byte-to 0.1 --cpu-multiplier 8 --mem-multiplier 12800000 --instances-multiplier-from 20 --instances-multiplier-to 20
```
