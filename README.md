# Go Mades Bench

This is a benchmark for testing message throughput for applications implementing the MADES Communication Standard

## Building

A linux binary can be compiled using the `make build` command and will be placed in `bin/go-mades-bench`

## Usage


```
> ./go-mades-bench -h

Usage of ./go-mades-bench:
  -goroutines int
        Number of go routines to use when sending (default CPU Thread Count)
  -inbox string
        inbox queue (default "ecp.endpoint.inbox")
  -inbox-addr string
        Socket address to reach the internal broker (default "amqp://localhost:5672")
  -inbox-user string
        Inbox broker password (default "endpoint")
  -inbox-pass string
        Inbox broker username (default "password")
  -max-in-transit int
        Max messages allowed in transit. max-in-transit <= 0 means unlimited
  -message-type string
        Message type to send messages with (default "TEST-MESSAGE")
  -n int
        Number of messages to send (default 10000)
  -outbox string
        outbox queue (default "ecp.endpoint.outbox")
  -outbox-addr string
        Socket address to reach the internal broker (default "amqp://localhost:5672")
  -outbox-pass string
        Outbox broker password (default "password")
  -outbox-reply string
        outbox reply queue (default "ecp.endpoint.outbox.reply")
  -outbox-user string
        Outbox broker username (default "endpoint")
  -receiver string
        Receiver Component Code (default "ecp-endpoint")
  -send-event string
        send event queue (default "ecp.endpoint.send.event")
  -size int
        Incompressible payload size to generate (default 1000000)
```

## Example

``` log
./go-mades-bench -n=250 -size=2500 -goroutines=1 -receiver=ecp-endpoint -inbox-addr=amqps://0.0.0.0:11005 -inbox-user=endpoint -inbox-pass=password -outbox-addr=amqps://0.0.0.0:11005"
...
2022/08/30 11:15:30 Started at       : 2022-08-30 11:14:20.036104864 +0200 CEST m=+3.041167580
2022/08/30 11:15:30 Ended at         : 2022-08-30 11:15:29.23447968 +0200 CEST m=+72.239542396
2022/08/30 11:15:30 Duration         : 1m9.198374816s
2022/08/30 11:15:30 Msg count        : 250 
2022/08/30 11:15:30 Msg size         : 2500 bytes
2022/08/30 11:15:30 Msg Throughput   : 3.612802 msgs/s
2022/08/30 11:15:30 Data Throughput  : 0.009032 mb/s (estimated)
2022/08/30 11:15:30 Msg transit time : 23.918915777s (avg) 23.447003503s (median) for Received
2022/08/30 11:15:30 Msg transit time : 44.564976534s (avg) 51.256929606s (median) for Delivery Event
2022/08/30 11:15:30 Msg transit time : 44.692382424s (avg) 51.326928701s (median) fpr Received Event
```