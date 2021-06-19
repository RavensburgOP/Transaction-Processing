# Transaction Processing

Simple implementation of a transaction processing system

## Assumptions

- I've assumed, that no transactions can be performed on a locked account

## Design

I've used the type system to ensure correctness for most of the code and written a few sanity checks to ensure correct output for simple cases

I convert the amounts to u64, because floats can result in rounding erros, which is a critical error in a financial system, and because integer operations on most systems are faster than float operations. This restricts the system in some ways, as division is not possible with a naive implementation, but since the system only handles addition and subtraction, this seemed like a worthwhile trade-off.

The input file is read into a buffer to avoid exceeding the memory limit of the system.
