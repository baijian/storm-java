
### RabbitMQ

**Server(Broker)**

backup003(10.31.22.88|117.121.6.88)

**ManagementPlugin**

http://117.121.6.88:15673


### Storm UI

http://117.121.6.88:8080

### AlogCountTopology Architecture


    -----------      ---------------      ------------------      ----------------------
    |         |      |             |      |                |      |                    |
    |RabbitMQ |----->|Spout(no ack)|----->|Bolt(filter uri)|----->|Bolt(count and save)|
    |         |      |             |      |                |      |                    |
    -----------      ---------------      ------------------      ----------------------
                                                 |
                                                \|/
                                              --------------
                                             /synchronous  /
                                            /uris every 3m/
                                            --------------


### SaveLogTopology Architecture




### Deployment
                                            
**backup003(117.121.6.88)**

Nimbus, StormUI, Zookeeper

**backup005(10.31.22.81)**

Zookeeper, Supervisor



