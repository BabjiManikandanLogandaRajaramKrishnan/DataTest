# MetricsBuilder

Steps for Job Execution in Local Environment:

Point Metrics:

1. Add SOURCE_PATH in config.properties
2. Execute spark job through IDE or spark-submit in shell.   


Campaign Metrics:

1. Add following properties in config.properties
    CONTEXT_SOURCE_PATH - Context Source(Local Source Path)
    EVENT_SOURCE_PATH - Event Source(Local Source Path)
    TARGET_PATH - HDFS File write path(Local target Path)

2. Execute spark job through IDE or spark-submit in shell.   


Flink Streaming:

1. Add BOOTSTRAP_SERVER, TOPIC details in config.properties
2. Execute Streams by setting up local kafka environment.
