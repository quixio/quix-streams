# Checkpointing

Quix Streams allows you to do manual checkpointing when you read data from a Topic. This gives you the ability to inform the message broker that you have already processed messages up to a point, usually called a **checkpoint**.

This is a very important concept when you are developing high-performance, streaming applications, processing lots of data in memory. You don’t want to persist a state for each message received because it would cause an unaffordable processing cost, slowing down your streaming speeds and performance.

Checkpointing lets you do some of this costly processing at a lower frequency, without having to worry about losing data. If, for some reason, your process is restarted or crashes and you haven’t saved all the in-memory data you are processing, the message broker will resend all the messages from the last Checkpoint when you reconnect to the topic.

Refer to the [Committing / checkpointing](subscribe.md#committing-checkpointing) section of this documentation to find out how to do Checkpointing when reading data with the library.

## Checkpointing example

Let’s explain the checkpointing concept and its benefits with an easy example.

One process is reading and processing data, without saving its state after each message received. This allows good performance and high throughtput of the service but, without checkpointing, risks data loss in the case of failure.

1.  The process reads the first four messages, keeping its state in memory.
    
    ![Reading first four messages](../images/Checkpointing1.png)

2.  The process commits the messages of the topic (checkpointing) just after reading the first four and saves the in-memory state to the database.
    
    ![Checkpointing marking messages already processed](../images/Checkpointing2.png)

3.  The process reads the next four messages, but it crashes just after that, without time to commit the messages. This will not result in data loss because it will begin from the last checkpoint after it restarts.
    
    ![Process crashes without commiting last messages](../images/Checkpointing3.png)

4.  The process restarts and reopens the input topic. It will start reading messages from the last checkpoint resulting in no data loss from the previous crash.
    
    ![Process recover itself after restart](../images/Checkpointing4.png)

5.  The process resumes reading the next five messages, keeping its state in memory.
    
    ![Process resumes reading next five messages](../images/Checkpointing5.png)

6.  The process commits the messages of the topic just after reading the previous five messages and saves the in-memory state to the database.
    
    ![Final checkpointing](../images/Checkpointing6.png)
