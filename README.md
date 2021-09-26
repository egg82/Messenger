# Messenger

Backend messaging system for Java stuffs

## Usage

```Java
// Set our server ID
UUID serverId = UUID.randomUUID();

// Create a new non-redundant PacketService with 4 sending threads and a packet version of 1.
PacketService packetService = new PacketService(4, false, (byte) 1);
```

```Java
// Create a new messaging handler implementation
MessagingHandlerImpl handlerImpl = new MessagingHandlerImpl(packetService);

// Create a new server messaging handler implementation - should extend AbstractServerMessagingHandler
AbstractMessagingHandler serverHandler = new ServerMessagingHandlerImpl(serverId, packetService, handlerImpl);

// Add the server messaging handler to the messaging handler implementation
handlerImpl.addHandler(serverHandler);

// Create your own custom messaging handler - should extend AbstractMessagingHandler
AbstractMessagingHandler customHandler = new CustomMessagingHandler(packetService);

// Add the custom messaging handler to the messaging handler implementation
handlerImpl.addHandler(customHandler);
```

```Java
// Create a new RabbitMQMessagingService
// The name of this service is "engine1" and uses the RabbitMQ channel "my-data" for all packets
// This service will have no startup delay (ie. will instantly start receiving packets)
// This service will *not* dump packets to disk, although the directory for those dumped packets will be packetDir
// Dumping packets is useful for Zstd training
RabbitMQMessagingService rabbit = RabbitMQMessagingService.builder(packetService, "engine1", "my-data", handlerImpl, 0L, false, packetDir)
    .url("127.0.0.1", 5672, "/") // Connect to 127.0.0.1:5672 @ vhost "/"
    .credentials("guest", "guest") // With guest user/pass
    .timeout(5000) // With a timeout of 5 seconds
    .build();

// Queue a packet through the packetservice
packetService.queuePacket(new KeepAlivePacket(serverId));

// Queue several packets through the packetservice
packetService.queuePackets(
    new InitializationPacket(serverId, packetService.getPacketVersion())
    new CustomDataPacket()
);

// Flush the packetService queue to actually send any packets that have been previously queued
packetService.flushQueue();
```

## Notes

* `AbstractServerMessagingHandler` will remove any servers that have not broadcasted `KeepAlivePacket`s in 20 seconds.
* `AbstractServerMessagingHandler` requires `InitializationPacket`s to be sent on startup, and `PacketVersionPacket` responses to `PacketVersionRequestPacket`s
* `AbstractServerMessagingHandler` prefers handling `ShutdownPacket`s over a missed `KeepAlivePacket` on shutdown
* `PacketService#queuePacket` and `PacketService#queuePackets` **do not actually broadcast packets** - `PacketService#flushQueue` will perform the broadcasting functionality with any packets queued. This allows for batching packets into a `MultiPacket` automatically