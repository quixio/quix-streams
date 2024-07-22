# Checkpointing

To process data reliably and with consistent outputs, Quix Streams applications need to periodically save their state stores to disk and commit processed offsets to Kafka.

We call this process a “checkpointing”.

The goal of checkpointing is to ensure that applications can recover after failures and reprocess records from Kafka producing the same results as if the failure never happened.


Quix Streams supports both *At-Least-Once* and *Exactly-Once* processing guarantees, which can be changed using the `processing_guarantee` parameter.

- When At-Least-Once guarantee is enabled (the default), each incoming message is guaranteed to be processed, but it may happen multiple times and generate duplicated outputs in case of failure
- If Exactly-Once guarantee is enabled, the outputs are guaranteed to be unique for every message at the cost of increased latency.

See the [Configuration](../configuration.md#processing-guarantees) page to learn more about processing guarantees.  
 

## Under the Hood

- When `Application` starts processing, it initializes the `Checkpoint` object with a fixed lifetime interval in seconds according to the `commit_interval` setting (every 5 seconds by default).
- The `Checkpoint` object is responsible for keeping track of processed Kafka offsets and pending state transactions.
- After the message is successfully processed, its offset is marked as processed in the current checkpoint.
- When checkpoint commits, it will:
    1. *Produce changelog messages for every pending state update to the changelog topics (if they are enabled).*
    2. *Flush the Kafka Producer and verify the delivery of every outgoing message both to output and changelog topics.*
    3. *Synchronously commit the topic offsets to Kafka.*
    4. *Flush the pending state transactions to the durable state stores.*
        
- After the checkpoint is fully committed, a new one is created and the processing continues.
- Besides the regular intervals, the checkpoint is also committed when Kafka partitions are rebalanced.

## Recovering the State Stores

In Quix Streams, all state stores are backed up using the changelog topics in Kafka.

Since version 2.5, all changelog messages contain the corresponding offsets of input topic messages.

During recovery, these offsets are compared with the currently committed offset of the input topic.

If the processed offset in the changelog record is higher than the current committed offset (i.e., the update was produced to the changelog, but the Kafka offsets failed to commit), this
changelog update will not be applied to the state.

This way, all stateful operations will work with a consistent snapshot of the state and produce the same outputs in case of reprocessing.

For more information about changelog topics, see the [**How Changelog Topics Work**](stateful-processing.md#how-changelog-topics-work) section.

## Common Failure Scenarios
Below are some examples of what can go wrong during processing and how application will be recovering from it.  
In all the cases, the application stops, and it needs to be restarted.

- **Error happens during processing of the message**.  
The app will stop without producing changelogs and committing offsets.  
On restart, it will restart processing from the latest committed offset.

- **Checkpointing fails to produce some state updates to the changelog topics.**   
Input topic offsets will not be committed, and the local state stores will not be flushed.  
On recovery, application will ignore the changelog updates belonging to non-committed input topic offsets.  
After recovery, the app will restart processing from the latest committed offset.

- **Checkpointing fails to commit offsets to the input topics.**   
Input topic offsets will not be committed, and the local state stores will not be flushed.  
On recovery, application will ignore the changelog updates belonging to non-committed input topic offsets.  
After recovery, the app will restart processing from the latest committed offset.  

- **Checkpointing fails to flush state stores to the disk.**  
All changelog updates are produced, and the input topic offsets are committed. 
During recovery, the app will apply changelog updates to the local state stores.

## Configuring the Checkpointing

Users may configure how often the checkpoints are committed by passing the `commit_interval` and `commit_every` parameters to the `Application` class.

By default, the `commit_interval` is 5 seconds, and the `commit_every` is 0, and only the commit interval is taken into account. 

Changing the commit interval will have several implications for the application:

- ***Longer commit intervals*** will make the application flush state stores and changelogs less often, limiting the IO.
    
    At the same time, it may lead to larger memory usage because more state updates will be accumulated in memory.
    
    The required amount of memory depends on how many unique keys are processed (state transactions batch updates per key).
    
    Also, when an application fails, it will need to reprocess more messages since the latest checkpoint, increasing the number of duplicates produced to the output topics.
    
- ***Shorter commit intervals*** will lead to more IO because changelogs and state stores will be flushed more frequently.
    
    However, it will reduce memory usage and limit the number of potentially reprocessed messages, reducing duplicates.
    
- If `commit_interval` is set to `0`, the application will commit a checkpoint for every processed Kafka message.
- If `commit_every` is set, the application will commit after processing N messages across all assigned partitions.
  - You may use `commit_interval` to get more granular control over the commit schedule.  
  - For example, if `commit_every=1000` and `commit_interval=5.0`, the application will commit the checkpoint as soon as 1000 messages are processed or 5s interval is elapsed. 

When configuring the `commit_interval` and `commit_every`, take into account such factors as the number of unique keys in the input topics, hardware, and infrastructure.


## Limitations

In the At-Least-Once setting, it is still possible that unwanted changelog changes get applied during recovery from scratch. 

**Example:**

- The checkpoint successfully produces changelog updates and flushes the Producer.
- The checkpoint fails to commit the input topic offsets to Kafka.
- The application code changes and some of the input messages get filtered during reprocessing.
- Since the changelogs are already produced, during recovery from scratch they will be applied to the state even though the messages are now filtered.

Though this case is rare, the best way to avoid it is to stop the application clean and ensure the latest checkpoint successfully commits before updating the processing code.

