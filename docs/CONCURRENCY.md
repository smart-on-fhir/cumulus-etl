# Concurrency in Cumulus ETL

Concurrency is a general term for running code in parallel.

Python has [several specific strategies](https://realpython.com/python-concurrency/)
for achieving concurrency, each with its own pros and cons.
But in brief, you can use multiple CPUs, threads, or cooperative multitasking.

Each makes more sense in different performance profiles.
Cumulus ETL itself is broadly I/O bound, rather than CPU bound.
But let's talk about the various pieces of the whole.

## Performance Profile of Cumulus ETL Steps

Obviously, this document might become out of date, so we won't get too in the weeds.

### I2B2 Conversion

This can't be done in parallel with other steps.
It's almost entirely about reading, converting, and writing as fast as it can.

It might benefit from some concurrency, but this step becomes less important with bulk FHIR.

### MS Tool De-Identification

We can't control concurrency of this, and we need its output before we do any other steps.

Thankfully, the MS tool does use multiple CPUs already.

### Tasks

Each task is broadly independent, with just the codebook as shared writable data.
Most tasks are just reading, de-identifying, and writing as fast as they can.
Only the NLP tasks do something interesting with the input data.

### NLP

cTAKES uses multiple cores already.
I'm not sure about cNLP transformers, but ideally you're using the GPU for that.

### Delta Lake

Delta Lake can handle multiple requests at once and sends them to another Java process,
which does use multiple CPUS.

Delta Lake technically allows multiple writers at once to the same table,
but only in the sense that nothing will be corrupted.
If you realistically try to do that, one of your writers will get an error.

So writes to Delta Lake inside a task should be in serial.
But multiple tasks can each send requests to Delta Lake for their own tables in parallel.

## Memory

One thing to be mindful of as you increase concurrency is that each active task needs data.
If you have a bundle size of 200k, each task would (naively) want its own 200k to operate on.

So you either need to document that interaction between bundle size and concurrency,
or secretly share the bundle size between concurrent tasks.

Smaller bundles are mostly tolerable, since our most important format writer (Delta Lake) can
consolidate small files into larger ones.
(If we didn't do that, we might be hurting performance on the query side of things.)

## Approaches

Since we're mostly just reading, writing, and sending off tasks to other processes (NLP & Delta),
it's safe to say that using multiple cores is probably not our highest priority.
Especially since that would add some complexity (each process would need its own copies of some
data like the pyspark instance).

Instead, threads are a bit easier to understand and get us the same benefit of waiting on some I/O.

Asyncio is another similar approach that would work,
but it needs the cooperation of your I/O libraries and pyspark is not compatible.

Here's what we do now for concurrency:
- Run each task on its own thread

Future improvements might be:
- Sending multiple NLP requests from the same task at the same time
- Reading the next bundle while waiting for a Delta Lake write to finish
