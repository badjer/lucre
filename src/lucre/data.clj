(ns lucre.data
  (:require [gloss.core :as gloss]
            [gloss.io :as gloss.io]))


(defn from-ms-ticks [ticks]
  "Converts a date in MS ticks into a java Date"
  (java.util.Date. (long (/ (- ticks 634200192000000000)
                            10000))))

(defn- decode [rec]
  (let [res (apply hash-map rec)]
    (assoc res :idate (from-ms-ticks (:iticks res)))))

(def ntd-codec (gloss/compile-frame
                 [:pm :float64-le
                  :junk1 :uint32-le
                  :count :uint32-le
                  :iprice :float64-le
                  :iprice2 :float64-le
                  :iprice3 :float64-le
                  :iprice4 :float64-le
                  :iticks :uint64-le
                  :ivol :uint64-le]
                 identity
                 decode))


(defn ntd-file [file]
  (let [header (byte-array 64)
        _ (.read (clojure.java.io/input-stream file) header)]
    ; TODO: Parse the rest of the file as well
    [(gloss.io/decode ntd-codec header)]))
