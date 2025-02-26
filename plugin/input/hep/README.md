# HEP plugin
Reads events from HEP requests.

Also, it emulates some protocols to allow receiving events from a wide range of software that use HEP to transmit data.
E.g. `file.d` may pretend to be Homer allows clients to send events using Homer protocol.
So you can use Homer output plugin to send data to `file.d`.

> ⚠ Currently event commitment mechanism isn't implemented for this plugin.
> Plugin answers with HTTP code `OK 200` right after it has read all the request body.
> It doesn't wait until events are committed.

**Example:**
Emulating elastic through hep:
```yaml
pipelines:
  example_k8s_pipeline:
    settings:
      capacity: 1024
    input:
      # define input type.
      type: hep
      # pretend homer search, emulate it's protocol.
      emulate_mode: "homer"
      # define hep port.
      address: ":9063"
    actions:
      # parse elastic search query.
      - type: parse_es
      # decode elastic search json.
      - type: json_decode
        # field is required.
        field: message
    output:
      # Let's write to kafka example.
      type: kafka
        brokers: [kafka-local:9092, kafka-local:9091]
        default_topic: yourtopic-k8s-data
        use_topic_field: true
        topic_field: pipeline_kafka_topic

      # Or we can write to file:
      # type: file
      # target_file: "./output.txt"
```

Setup:
```
# run server.
# config.yaml should contains yaml config above.
go run cmd/file.d.go --config=config.yaml

# now do requests.
curl "localhost:9200/_bulk" -H 'Content-Type: application/json' -d \
'{"index":{"_index":"index-main","_type":"span"}}
{"message": "hello", "kind": "normal"}
'

##


### Config params
**`address`** *`string`* *`default=:9200`* 

An address to listen to. Omit ip/host to listen all network interfaces. E.g. `:88`

<br>

**`emulate_mode`** *`string`* *`default=no`* *`options=no|elasticsearch`* 

Which protocol to emulate.

<br>

**`ca_cert`** *`string`* 

CA certificate in PEM encoding. This can be a path or the content of the certificate.
If both ca_cert and private_key are set, the server starts accepting connections in TLS mode.

<br>

**`private_key`** *`string`* 

CA private key in PEM encoding. This can be a path or the content of the key.
If both ca_cert and private_key are set, the server starts accepting connections in TLS mode.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*
