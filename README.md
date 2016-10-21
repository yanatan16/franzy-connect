# NOTE: This is a PROPOSAL for a franzy project. It is not associated with franzy (yet)

# franzy-connect

A clojurized kafka connect interface.

This project aims to cover up all the java nonsense in a nice and clojure way. This library aims to help manage deferred completion of writes and retry logic.

## Install

```
[org.clojars.yanatan16/franzy-connect "0.1.0"]
```

## Usage

``` clojure
(require '[franzy.connect :as c])

(c/make-simple-sink
 org.very.long.package.path.MySinkConnector
 (fn [{:keys [key value topic partition]}]
   (write-thing key value))

(c/make-simple-source
 org.very.long.package.path.MySourceConnector
 (fn [] (read-thing)))
```

## License

See license in [LICENSE](/LICENSE) file.
