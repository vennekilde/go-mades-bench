# Go Mades Bench

This is a benchmark tool for testing message throughput for applications implementing the [Energy Communication Platform (ECP)](https://www.entsoe.eu/ecco-sp/info/) variant of the [MADES Communication Standard](https://eepublicdownloads.entsoe.eu/clean-documents/EDI/Library/depreciated/503_mades-v1r1.pdf)

The tool uses the AMQP Business Application (BA) API in order to send & receive messages and measures the timestamps of each AMQP event (create, send, inbox, delivered, received)

## Installation

Install using `go install` or download source code and build it on your machine by following the `Building` section

```bash
go install github.com/vennekilde/go-mades-bench@latest
```

## Building

``` bash
# Native OS build
make build
# Linux and Windows build
make build_all
# Linux  build
make build_linux
# Windows build
make build_windows
```

## Docker 

If you need to run the CLI tool in a docker container, you can run the docker image using the following commands:

```bash
# Launch container and name it go-mades-bench
docker run -d --name go-mades-bench ghcr.io/vennekilde/go-mades-bench:v1.0.0
# Create a shell within the newly launched container
docker exec -it go-mades-bench /bin/bash
```

## Usage

```
> go-mades-bench -h

Usage of go-mades-bench:
  -durable
        Should AMQP messages sent to the API broker be persisted (default true)
  -goroutines uint
        Number of go routines to use when sending (default "number of CPU vCores")
  -inbox string
        inbox queue (default "ecp.endpoint.inbox")
  -inbox-addr string
        Socket address to reach the internal broker (default "amqp://localhost:5672")
  -inbox-pass string
        Inbox broker username (default "password")
  -inbox-user string
        Inbox broker password (default "endpoint")
  -max-in-transit uint
        Max messages allowed in transit. max-in-transit <= 0 means unlimited (default 1000)
  -message-type string
        Message type to send messages with (default "TEST-MESSAGE")
  -mode string
        mode (endpoint, toolbox, amqp, tracing) (default "endpoint")
  -n uint
        Number of messages to send (default 10000)
  -outbox string
        outbox queue (default "ecp.endpoint.outbox")
  -outbox-addr string
        Socket address to reach the internal broker (default "amqp://localhost:5672")
  -outbox-pass string
        Outbox broker password (default "password")
  -outbox-reply string
        outbox reply queue (default "ecp.endpoint.outbox.reply")
  -outbox-send-event string
        send event queue (default "ecp.endpoint.send.event")
  -outbox-user string
        Outbox broker username (default "endpoint")
  -receiver string
        Receiver Component Code (default "ecp-endpoint")
  -size uint
        Incompressible payload size to generate (default 1000000)
  -v    Print all message events
```

## Example

Benchmark example where 10000 messages are sent with a size of 3kb

``` log
> go-mades-bench -n=10000 -size=3000 -receiver=ecp-endpoint

...
2022/11/22 11:42:26 Final Report
2022/11/22 11:42:26 ==============================================================================
2022/11/22 11:42:26 Started at       : 2022-11-22T11:16:58+01:00
2022/11/22 11:42:26 Ended at         : 2022-11-22T11:42:25+01:00
2022/11/22 11:42:26 Duration         : 25m27.035791317s
2022/11/22 11:42:26 Msg size         : 3000 bytes
2022/11/22 11:42:26 
2022/11/22 11:42:26 === Statistics
2022/11/22 11:42:26 Sent to outbox   : 10.817 msgs/s	 ~0.032 mb/s	 10000/10000 msgs	 duration: 15m24.466s	 -- not applicable --
2022/11/22 11:42:26 Sent to broker   : 10.815 msgs/s	 ~0.032 mb/s	 10000/10000 msgs	 duration: 15m24.681s	 flight time: 57.590 (avg) 21.925 (median)
2022/11/22 11:42:26 Received in inbox: 7.321 msgs/s	 ~0.022 mb/s	 10000/10000 msgs	 duration: 22m45.945s	 flight time: 339.900 (avg) 380.086 (median)
2022/11/22 11:42:26 Delivery Event   : 6.549 msgs/s	 ~0.001 mb/s	 10000/10000 msgs	 duration: 25m26.948s	 flight time: 650.113s (avg) 693.767s (median)
2022/11/22 11:42:26 Received Event   : 6.549 msgs/s	 ~0.001 mb/s	 10000/10000 msgs	 duration: 25m27.036s	 flight time: 650.170s (avg) 692.669s (median)
2022/11/22 11:42:26 ==============================================================================
```

## Example with 100 million messages

Example of final report after sending 100 million messages with a size of 3kb and a maximum of 5000 messages allowed to be unacknowledged to prevent flooding the internal broker, as the benchmark can push messages way faster, than the endpoint can process them.

Note: this test was done using a custom implementation of MADES/ECP, as sending 100 million messages using ECP, would take months and careful tuning to even succeed in running to completion.

``` log
> go-mades-bench -n=100000000 -size=3000 -max-in-transit=5000 -receiver=ecp-endpoint

...
2022/11/22 09:59:40 Final Report
2022/11/22 09:59:40 ==============================================================================
2022/11/22 09:59:40 Started at       : 2022-11-21T16:13:25+01:00
2022/11/22 09:59:40 Ended at         : 2022-11-22T09:59:39+01:00
2022/11/22 09:59:40 Duration         : 17h46m13.539112429s
2022/11/22 09:59:40 Msg size         : 3000 bytes
2022/11/22 09:59:40 
2022/11/22 09:59:40 === Statistics
2022/11/22 09:59:40 Sent to outbox   : 1563.177 msgs/s	 ~4.690 mb/s	 100000000/100000000 msgs	 duration: 17h46m12.269s	 -- not applicable --
2022/11/22 09:59:40 Sent to broker   : 1563.177 msgs/s	 ~4.690 mb/s	 100000000/100000000 msgs	 duration: 17h46m12.272s	 flight time: 0.020 (avg) 0.795 (median)
2022/11/22 09:59:40 Received in inbox: 1563.152 msgs/s	 ~4.689 mb/s	 100000000/100000000 msgs	 duration: 17h46m13.292s	 flight time: 1.611 (avg) 1.578 (median)
2022/11/22 09:59:40 Delivery Event   : 1563.146 msgs/s	 ~0.156 mb/s	 100000000/100000000 msgs	 duration: 17h46m13.539s	 flight time: 3.192s (avg) 3.136s (median)
2022/11/22 09:59:40 Received Event   : 1563.146 msgs/s	 ~0.156 mb/s	 100000000/100000000 msgs	 duration: 17h46m13.539s	 flight time: 3.208s (avg) 3.144s (median)
2022/11/22 09:59:40 ==============================================================================
```
