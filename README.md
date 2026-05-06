# README

Brandon Montalvo and Franky Irazaba

## Special Instructions

No special instructions. Build with:

make

Run with:

./scheduler_sim --policy <ALGORITHM> --input <workload.txt> --cpus <N> --quantum <Q>

`--quantum` is required only for RR.

## Question

Busy waiting is when a thread continuously checks a condition in a loop instead of sleeping, 
causing it to repeatedly use CPU cycles without doing useful work.

Example of busy waiting in a CPU worker thread:

```c
while (!work_available && !shutdown) {
    // keep checking without blocking
}