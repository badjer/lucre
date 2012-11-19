(ns lucre.topologytests
  (:require [backtype.storm.testing :as storm])
  (:use [lucre.topology]
        [midje.sweet]))


(facts "ma works"
  (ma []) => nil
  (ma [11M]) => 11M
  (ma [11M 13M]) => 12M)


(facts "enqueue works"
  (let [q (clojure.lang.PersistentQueue/EMPTY)
        q1 (conj q 1)
        q2 (conj q1 2)]
    (enqueue q 5 1) => [1]
    (enqueue q 1 1) => [1]
    (enqueue q1 1 2) => [2]
    (enqueue q2 2 3) => [2 3]))
  

(fact "create topology"
  (ma-topology) => truthy)

;(storm/with-local-cluster [cluster]
  ;(let [output (storm/complete-topology cluster
                                        ;(ma-topology)
                                        ;:mock-sources
                                        ;{"ticks" [[123M]
                                                  ;[125M]]})]
    ;(facts "ma-topology computes moving average"
      ;output => truthy
      ;(storm/read-tuples output "avg") => [[123M] [124M]])))
