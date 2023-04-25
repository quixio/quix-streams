# State management

Your code may get restarted multiple times. A user intervention (like manually stopping and starting) or runtime error could cause your application to terminate. 

!!! note

	When using the Quix Platform, the platform automatically detects the problem and restarts the underlying service in an attempt to recover from the fault.

Due to the code being run in memory, each time a deployment restarts, internal variables will be reset. For example, if you were to calculate the count of the elements in the stream, this counter would get reset on each restart. The counter would then start at the default value not knowing what was the last known value in the state of the previous run before program terminated.

Quix Streams has state management built in to enable values to be used and persisted across restarts of a given deployment. Quix Streams persists your state using your filesystem at the moment.

!!! note

	When using the Quix Platform, the platform provides your replicas with a shared state store when enabled.

## Usage

To use the library’s state management feature, create an instance of `LocalFileStorage`, and then use the available methods on the instance to manipulate the state as needed. For example:

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

## In memory storage

In Python there is another storage option available as an experimental feature called `InMemoryStorage`. It works in a similar way to `LocalFileStorage` and also supports dictionary operations such as `del` or iteration.

`InMemoryStorage` can be used on its own, as demonstrated in the following example:

``` python
from quixstreams import InMemoryStorage

storage = InMemoryStorage()
storage.clear()
storage.set("floatval", 12.51)
storage.set("stringval", "str")
storage.set("boolval", True)
storage.set("objval", {"dic": "tionary"})
```

It can also be used in conjunction with `LocalFileStorage`, as shown in the following example:

``` python
from quixstreams import InMemoryStorage, LocalFileStorage

storage = InMemoryStorage(LocalFileStorage("state/test"))
storage.clear()
storage.set("floatval", 12.51)
storage.set("stringval", "str")
storage.set("boolval", True)
storage.set("objval", {"dic": "tionary"})
storage.flush()  # to write to backing storage

# can be useful to hook it to consumer.on_committing
topic_consumer.on_committing = storage.flush
# or 
def on_committing_handler(topic_consumer: qx.TopicConsumer):
    print("Committing!")
    storage.flush()  # to write to backing storage

topic_consumer.on_committing = on_committing_handler
```