(ns com.hello.messeji.config
  (:require
    [clojure.edn :as edn]
    [schema.core :as s])
  (:refer-clojure :exclude [read]))

(def Config
  "Schema for the configuration map."
  {:key-store {:table s/Str
               :endpoint s/Str}
   :http {:pub-port s/Int
          :sub-port s/Int
          :receive-timeout s/Int}
   :logging {:property-file-name s/Str
             :properties {:log-level s/Str}}
   :redis {:spec {s/Keyword s/Any} ;; See documentation for carmine/wcar spec
           :delete-after-seconds s/Int}
   :max-message-age-millis s/Int
   :request-log {:stream s/Str  ; Kinesis stream name to use
                 ;; Log level for the KinesisProducer. Should be info|warning|error
                 :log-level s/Str}
   :graphite {:host s/Str
              :api-key s/Str
              :port s/Int
              :env s/Str
              :period s/Int   ; Reporting interval in seconds
              :enabled? s/Bool}})

(defn- deep-merge
  "Deeply merges maps so that nested maps are combined rather than replaced.
  For example:
  (deep-merge {:foo {:bar :baz}} {:foo {:fuzz :buzz}})
  ;;=> {:foo {:bar :baz, :fuzz :buzz}}
  ;; contrast with clojure.core/merge
  (merge {:foo {:bar :baz}} {:foo {:fuzz :buzz}})
  ;;=> {:foo {:fuzz :quzz}} ; note how last value for :foo wins"
  [& vs]
  (if (every? map? vs)
    (apply merge-with deep-merge vs)
    (last vs)))

(defn- read-edn-file
  [file-name]
  (-> file-name slurp edn/read-string))

(defn read
  "Given a seq of file names, return the parsed config map.
  Files will be deeply merged over each other, so (read [\"x.edn\" \"y.edn\"])
  means that values from y.edn will override those in x.edn."
  [& file-names]
  (->> file-names
    (map read-edn-file)
    (apply deep-merge)
    (s/validate Config)))
