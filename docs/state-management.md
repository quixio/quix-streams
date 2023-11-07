# State management

Your code may get restarted multiple times. A user intervention (like manually stopping and starting) or runtime error could cause your application to terminate. 

!!! note

	When using the Quix Platform, the platform automatically detects the problem and restarts the underlying service in an attempt to recover from the fault.

Due to the code being run in memory, each time a deployment restarts, internal variables will be reset. For example, if you were to calculate the count of the elements in the stream, this counter would get reset on each restart. The counter would then start at the default value not knowing what was the last known value in the state of the previous run before program terminated.

Quix Streams has state management built in to enable values to be used and persisted across restarts of a given deployment. Quix Streams persists your state using your filesystem at the moment.

!!! note

	When using the Quix Platform, the platform provides your replicas with a shared state store when enabled.

The library provides automatic state management which handles application lifecycle automatically, such as commits and revocation to ensure the state reflects the processed and committed messages only. There are two types of state available: dictionary state and scalar state. The stream state managed is available on stream consumer and not the producer currently.

## Reading and writing

=== "Python"
    ``` python
        # ... context where stream consumer is available such as on_stream_received, on_data_received handlers ...

        # This will return a state where type is 'Any'
        my_any_state = stream_consumer.get_dict_state('some_state')
        my_any_state['my_key'] = 42
        my_any_state['some_other_key'] = ['this', 'could', 'be', 'an', 'array', 'also']
        
        # this will return a state where type is a generic dictionary, with an empty dictionary as default value when
        # key is not available. The lambda function will be invoked with 'get_state_type_check' key to determine type
        my_nested_dict_state = stream_consumer.get_dict_state('some_state', lambda missing_key: return {})
        my_nested_dict_state['someParam']['RollingAverage'] = 37.872
        my_nested_dict_state['someParam']['LastValue'] = 6
        my_nested_dict_state['someParam']['Mean'] = 37


        # this will return a state where type is a specific dictionary type, with default value
        mt_nested_typed_dict_state = stream_consumer.get_dict_state('some_state', lambda missing_key: return {}, Dict[str, float])
        mt_nested_typed_dict_state['my_key']['my_nested_key'] = 37.872


        # this will return a state where type is a float without default value, resulting in KeyError when not found
        my_float_state = stream_consumer.get_dict_state('some_state', state_type=float)
        my_float_state['my_key'] = 3.14
        my_float_state['my_other_key'] = 3.15    
        
    ```
=== "C\#"
    ``` cs
        # ... context where stream consumer is available such as OnStreamReceived, OnDataReceived handlers ...

        # This will return a state where type is 'int'
        var myIntState = streamConsumer.GetDictionaryState<int>("RollingSum");   
        myIntState["my_key"] = 42
        myIntState["my_key"] += 13

        # this will return a state where type is a specific dictionary type, with default value
        var myLastValueState = streamConsumer.GetDictionaryState("LastValue", (missingKey) => new Dictionary<string, double>());
        myLastValueState["someParam"]["RollingAverage"] = 37.872
        myLastValueState["someParam"]["LastValue"] = 6
        myLastValueState["someParam"]["Mean"] = 37
    ```

## Querying

You can query the existing states several ways. All states can be iterated through starting from App, Topic or Stream.

=== "Python"
    ``` python
        # From app level
        import quixstreams as qx
        app_state_manager = qx.App.get_state_manager()
        topic_states = app_state_manager.get_topic_states() # returns all topic states (as string) that can be managed
        topic_state_manager = app_state_manager.get_topic_state_manager('my_topic')  # note, with Quix Manager broker, this would be topic id
        stream_state_manager = topic_state_manager.get_stream_state_manager('my_stream_id')
        stream_state = stream_state_manager.get_dict_state('some_state') # work same as in other samples
        stream_state_value = stream_state['my_key']

        # When topic consumer is available
        topic_state_manager = topic_consumer.get_state_manager()
        stream_states = topic_state_manager.get_stream_states() # returns all topic states (as string) that can be managed
        stream_state_manager = topic_state_manager.get_stream_state_manager('my_stream_id')
        stream_state = stream_state_manager.get_dict_state('some_state') # work same as in other samples
        stream_state_value = stream_state['my_key']

        # When stream consumer is available
        stream_state_manager = stream_consumer.get_state_manager()
        stream_states = topic_state_manager.get_states() # returns all stream states (as string) that can be managed
        # note, you can directly use stream_consumer.get_dict_state('some_state') instead if don't need other management API access
        stream_state = stream_state_manager.get_dict_state('some_state') # work same as in other samples
        stream_state_value = stream_state['my_key']
    ```
=== "C\#"
    ``` cs
        // From app level
        var appStateManager = App.GetStateManager();
        var topicStateManager = appStateManager.GetTopicStateManager("my_topic");  // note, with Quix Manager broker, this would be topic id
        var streamStateManager = topicStateManager.GetStreamStateManager("my_stream_id");
        var streamState = streamStateManager.GetDictionaryState<int>("some_state"); // work same as in other samples
        var streamStateValue = streamState["my_key"];

        // when topic consumer is available
        var topicStateManager = topicConsumer.GetStateManager();
        var streamStateManager = topicStateManager.GetStreamStateManager("my_stream_id");
        var streamState = streamStateManager.GetDictionaryState<int>("some_state"); // work same as in other samples
        var streamStateValue = streamState["my_key"];

        // when stream consumer is available
        var streamStateManager = streamConsumer.GetStateManager();
        // note, you can directly use streamConsumer.GetDictionaryState<int>("some_state") instead if don't need other management API access
        var streamState = streamStateManager.GetDictionaryState<int>("some_state"); // work same as in other samples
        var streamStateValue = streamState["my_key"];
    ```

## Deleting

You can delete any or all state using the state manager of a specific level. See [Querying](#querying) section for how to acquire specific managers.

=== "Python"
    ``` python
        # From app level
        import quixstreams as qx
        app_state_manager = qx.App.get_state_manager()
        app_state_manager.delete_topic_state('specific_topic')  # note, with Quix Manager broker, this would be topic id
        app_state_manager.delete_topic_states()  # deletes all

        # When topic consumer is available
        topic_state_manager = topic_consumer.get_state_manager()
        topic_state_manager.delete_stream_state('stream_id') 
        topic_state_manager.delete_stream_states()  # deletes all

        # When stream consumer is available
        stream_state_manager = stream_consumer.get_state_manager()
        stream_state_manager.delete_state('some_state') 
        stream_state_manager.delete_states()  # deletes all
    ```
=== "C\#"
    ``` cs
        // From app level
        var appStateManager = App.GetStateManager();
        appStateManager.DeleteTopicState("specific_topic"); // note, with Quix Manager broker, this would be topic id
        appStateManager.DeleteTopicStates();

        // when topic consumer is available
        var topicStateManager = topicConsumer.GetStateManager();
        topicStateManager.DeleteStreamState("stream_id");
        topicStateManager.DeleteStreamStates(); // deletes all

        // when stream consumer is available
        var streamStateManager = streamConsumer.GetStateManager();
        streamStateManager.DeleteState("some_state");
        streamStateManager.DeleteStates(); // deletes all
    ```

## Scalar state type
In addition to the dictionary state type, we also have the scalar state type. It functions similarly, but holds just a single value, making it simpler to use. Below is an example:

=== "Python"
    ``` python
    def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, data: qx.TimeseriesData):
        # Define a scalar state with a default value of 0.
        stream_state = stream_consumer.get_scalar_state("total_rpm", lambda: 0)

        # Iterate over all the timestamps in the data.
        for row in data.timestamps:
            # Extract the numeric value for 'EngineRPM'.
            rpm = row.parameters["EngineRPM"].numeric_value

            # Increment the state value by the extracted RPM.
            stream_state.value += rpm

            # Add the updated state value to the row as 'total_rpm'.
            row.add_value("total_rpm", stream_state.value)
    ```

## Storage types

Any state storage is supported as long as as it implements IStateStorage. These are currently LocalFileStorage and InMemoryStorage.

The storage type must be specified at app level using the following code, but by default LocalFileStorage is used at the moment.

=== "Python"
    ``` python
        import quixstreams as qx
        state_inmem_storage = qx.InMemoryStorage()
        qx.App.set_state_storage(state_inmem_storage) # this mostly makes sense for testing until other storage types are implemented
    ```
=== "C\#"
    ``` cs
        var storage = new InMemoryStorage();
        App.SetStateStorage(storage); // this mostly makes sense for testing until other storage types are implemented
    ```

## Using State storage directly

To use the library’s state management feature, create an instance of `LocalFileStorage` or `InMemoryStorage`, and then use the available methods on the instance to manipulate the state as needed. For example:

=== "Python"
    
    ``` python
    from quixstreams import LocalFileStorage
    
    storage = LocalFileStorage()
    
    #clear storage ( remove all keys )
    storage.clear()
    
    #storage class supports handling of
    #   `str`, `int`, `float`, `bool`, `bytes`, `bytearray` types.
    
    #set value
    storage.set("KEY1", 12.51)
    storage.set("KEY2", "str")
    storage.set("KEY3", True)
    storage.set("KEY4", False)
    
    #check if the storage contains key
    storage.contains_key("KEY1")
    
    #get value
    value = storage.get("KEY1")
    ```

=== "C\#"
    C\# supports two ways to call the Storage API.
    
      - Synchronous
    
      - Asynchronous ( methods are with Async suffix )
    
    The Synchronous API. During a call to these synchronous methods, the
    program thread execution is blocked.
    
    ``` cs
    var storage = new LocalFileStorage();
    
    //clear storage ( remove all keys )
    await storage.Clear();
    
    //set value to specific key
    await storage.Set("KEY1", 123);  //long
    await storage.Set("KEY2", 1.23); //double
    await storage.Set("KEY3", "1.23"); //string
    await storage.Set("KEY4", new byte[]{12,53,23}); //binary
    await storage.Set("KEY5", false); //boolean
    
    //check if the key exists
    await storage.ContainsKey("KEY1");
    
    //retrieve value from key
    await storage.GetLong("KEY1");
    await storage.GetDouble("KEY2");
    await storage.GetString("KEY3");
    await storage.GetBinary("KEY4");
    await storage.GetBinary("KEY5");
    
    //list all keys in the storage
    await storage.GetAllKeys();
    ```
    
    The asynchronous API in which methods do contain Async suffix. These methods use the Task-Based Asynchronous Pattern (TAP) and returnTasks. TAP enables Quix to use async / await and avoid blocking the main thread on longer-running operations. In this case internal I/O.
    
    ``` cs
    var storage = new LocalFileStorage();
    
    //clear storage ( remove all keys )
    await storage.ClearAsync();
    
    //set value to specific key
    await storage.SetAsync("KEY1", 123);  //long
    await storage.SetAsync("KEY2", 1.23); //double
    await storage.SetAsync("KEY3", "1.23"); //string
    await storage.SetAsync("KEY4", new byte[]{12,53,23}); //binary
    await storage.SetAsync("KEY5", false); //boolean
    
    //check if the key exists
    await storage.ContainsKeyAsync("KEY1");
    
    //retrieve value from key
    await storage.GetLongAsync("KEY1");
    await storage.GetDoubleAsync("KEY2");
    await storage.GetStringAsync("KEY3");
    await storage.GetBinaryAsync("KEY4");
    await storage.GetBinaryAsync("KEY5");
    
    //list all keys in the storage
    await storage.GetAllKeysAsync();
    ```
    
## State folder

In Quix Cloud, when state management is enabled for a deployment, Quix Streams uses a `state` folder to store data and files. If running Quix Streams locally, the `state` folder is automatically created for you. You can read more about enabling state, and using the `state` folder, in the [state management documentation](https://www.quix.io/docs/deploy/state-management.html).

