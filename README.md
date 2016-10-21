## NOTE: This is a PROPOSAL for a franzy project. It is not associated with [franzy](https://github.com/ymilky/franzy) (yet) ##

# franzy-connect

A clojurized kafka connect interface.

This project aims to cover up all the java nonsense in a nice and clojure way. This library aims to help manage deferred completion of writes and retry logic.

## Install

```
[org.clojars.yanatan16/franzy-connect "0.1.0"]
```

## Usage

You can make `SinkConnector`s:

``` clojure
(require '[franzy.connect.sink :as sink])

(c/make-sink
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

## TODO

- SourceConnectors
- Integration testing framework for testing your connectors

## License

See license in [LICENSE](/LICENSE) file.
