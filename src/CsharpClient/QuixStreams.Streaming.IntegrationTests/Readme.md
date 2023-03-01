# Dependencies

In order to run these tests, you must install docker host in your machine.

# Overview

A kafka docker container (with random ports) is spun up before any of the tests run. The tests make use of the kafka created this way.
See KafkaDockerTestFixture.cs for defining the collection and the fixture. Pay attention to the `[CollectionDefinition("somename")]` and `[Collection("somename")]` in tests. This is what links the collection to the tests. The container is only spun up once per collection, not per test.

# Known issues

Containers connected this way will be only cleaned up if the unit tests complete without interruption. If for any reason the unit test is not allowed to finish (such as you stop it), then the docker container will remain.