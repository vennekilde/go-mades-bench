# Go Mades Bench

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
        Receiver Component Code (default "TEST-CODE")
  -send-event string
        send event queue (default "ecp.endpoint.send.event")
  -size int
        Incompressible payload size to generate (default 1000000)
```