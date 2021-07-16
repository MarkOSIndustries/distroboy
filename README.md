# DistroBoy
A framework for distributing compute to a group of processes.

## Why yet another distributed compute project?
Yes, I know. Hadoop, Spark, Flink, and a bunch of other projects already fill this space.
Like most projects, this was borne of wanting something which didn't exist yet.
I wanted something which
- Doesn't carry with it the weight of Hadoop
  - setting up Hadoop is complicated and requires a bunch of stuff to already be there to work
- Does't carry with it the weight of Scala (just plain Java please)
  - I like using other JVM languages, but I don't want to *have* to use one.
- Can be deployed using Docker
  - I want to be able to run it the same way I run "normal" services, within reason
- Starts fast
- Avoids magic
  - I don't want to disguise side effects behind operations which look like familiar, simpler equivalents (like `groupBy`). I wanted something which made what was actually happening more explicit, at the cost of verbosity 

## Why the name?
It reminded me of the cartoon AstroBoy, and seemed inoffensive enough