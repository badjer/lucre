(ns lucre.topology
  (:require [backtype.storm.clojure :as storm]))

(storm/defspout tick-spout ["price"]
  [conf context collector]
  (let [ticks [123M 124M]]
    (storm/spout
      (storm/nextTuple []
                       (Thread/sleep 100)
                       (storm/emit-spout! collector [(first ticks)]))
      (storm/ack [id]))))


(defn ma [prices]
  (if (empty? prices) 
    nil
    (/ (apply + prices) (count prices))))

(defn enqueue [q qsize item]
  (let [res (conj q item)]
    (if (> (count res) qsize)
      (pop res)
      res)))


(storm/defbolt avg-bolt ["ma"] {:prepare true :params [size]}
  [conf context collector]
  (let [prices (atom [])]
    (storm/bolt 
      (execute [tuple]
               (let [price (tuple "price")]
                 ;(swap! prices enqueue size price)
                 (storm/emit-bolt! collector [(ma @prices)] :anchor tuple)
                 (storm/ack! collector tuple))))))


(defn ma-topology [] 
  (storm/topology
    {"ticks" (storm/spout-spec tick-spout)}
    {"avg" (storm/bolt-spec {"ticks" :shuffle}
                            avg-bolt)}))
