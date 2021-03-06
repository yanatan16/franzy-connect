## NOTE: This is a PROPOSAL for a franzy project. It is not associated with [franzy](https://github.com/ymilky/franzy) (yet) ##

# franzy-connect

A clojurized kafka connect interface.

This project aims to cover up all the java nonsense in a nice and clojure way.

## Install

```
[org.clojars.yanatan16/franzy-connect "0.1.0"]
```

## Usage

You can make `SinkConnector`s:

``` clojure
(require '[franzy.connect.sink :as sink])

(sink/make-sink
 org.very.long.package.path.MySink

 ;; Sink Task Methods
 {:put-1 (fn [config {:keys [key value topic partition]}]
           (write-thing key value))}

 ;; Sink Connector Methods
 {:config config})
```

You can also make `ConfigDef`s:

``` clojure
(require '[franzy.connect.config :as cfg :refer [make-config-def])

(def config
  (make-config-def
    (:my.key :type/string "default value" :importance/high "some comments")
    (:my.no-default-key :type/long ::cfg/no-default-value :importance/medium "docs")
    (:my.validated-key :type/string (cfg/validator-string-enum "foo" "bar")
                       :importance/medium "more docs")))
```

Then uberjar it up and stick on a connector classpath to use! See [docker-compose.yml](/docker-compose.yml) for an example.

## TODO

- SourceConnectors
- Integration testing framework for testing your connectors
- Use promises/deferreds to manage offset flushing in sinks

## Caveats

- Always mount your uber jars in `/usr/share/java/kafka-connect-<something>/<something>.jar`. Other things might not get loaded by the magic kafka connect classpath loader
- Don't use `clj-http`, its thread system doesn't like Kafka Connect. Try `aleph.http`

## License

See license in [LICENSE](/LICENSE) file.
