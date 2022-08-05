## A list of TODO tasks, ideas and known issues of sparkMeasure

Use this list as a reference for future development. 
If you plan to contribute to sparkMeasure development, please start by reviewing this list.

**Known issues and TODO list**
  * TODO: write more tests for GitHub CI actions.
  * TODO: Measure and report Task/stage failures and other errors that are not handled  by the code in this version, this puts the effort
    on the user to validate the output.
  * TODO: Task metrics values collected by sparkMeasure are only for successfully executed tasks. Note that 
    resources used by failed tasks are not collected in the current version. Can this be improved?
  * TODO: for Flight recorder mode with file output: implement writing metrics to output files incrementally, 
    rather than using the current approach of buffering everything in memory and writing at the end.
    Note flight recorder mode for Kafka and for InfluxDB does not suffer from this issue.
  * TODO: add code/exceptions to  handle error conditions that can arise in sparkMeasure code.
  * TODO: add more statistics related to job execution, for example report start/min/max.number of executors
    the job had, which is useful in the case of yarn with spark dynamic allocation.
