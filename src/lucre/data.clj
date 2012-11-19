(ns lucre.data
  (:use [gloss.core :only [header repeated defcodec enum finite-frame string
                           compile-frame defcodec-]]
        [gloss.io :only [decode decode-channel]]))


(def pricemult 1000000)

(defn fixprices [op fields rec]
  "Turns rec into a hashmap, and modifies the value of every field
  by applying op to them"
  (let [res (apply hash-map rec)]
    ; For every field in fields, change the value of 
    ; (field map) to (field map) / pricemult
    (reduce #(assoc %1 %2 (op (%2 %1))) res fields)))

(defn multprices [fields rec]
  (fixprices #(bigdec (* % pricemult)) fields rec))

(defn divprices [fields rec]
  (fixprices #(bigdec (/ % pricemult)) fields rec))

(defcodec- c-string (finite-frame :byte (string :utf-8)))

(def tick-header
  (compile-frame
    [:start :byte
     :version :uint32-le
     :symbol c-string
     :end :byte]
  identity
  #(apply hash-map %)))

(defcodec- ticktype (enum :byte 
                         {:startdata 1 :endtick 2 :enddata 3
                          :tickfull 32 :tickquote 33 :tickbid 34 :tickask 35
                          :ticktrade 36}))

(def tickask 
  (compile-frame 
    [:type :ask :date :int32-le :time :int32-le :ask :uint64-le :os :int32-le 
     :oe c-string :depth :int32-le :end :byte]
    (partial multprices [:ask])
    (partial divprices [:ask])))

(def tickbid
  (compile-frame 
    [:type :bid :date :int32-le :time :int32-le :bid :uint64-le 
     :bs :int32-le :be c-string :depth :int32-le :end :byte]
    (partial multprices [:bid])
    (partial divprices [:bid])))
    

(def tickfull 
  (compile-frame 
    [:type :full :date :int32-le :time :int32-le :trade :uint64-le :size :int32-le 
     :ex c-string :bid :uint64-le :bs :int32-le :be c-string :ask :uint64-le 
     :os :int32-le :oe c-string :depth :int32-le :end :byte]
    (partial multprices [:trade :bid :ask])
    (partial divprices [:trade :bid :ask])))

(def tickquote
  (compile-frame
    [:type :quote :date :int32-le :time :int32-le :bid :uint64-le :bs :int32-le 
     :be c-string :ask :uint64-le :os :int32-le :oe c-string :depth :int32-le :end :byte]
    (partial multprices [:bid :ask])
    (partial divprices [:bid :ask])))

(def ticktrade
  (compile-frame
    [:type :trade :date :int32-le :time :int32-le :trade :uint64-le :size :int32-le 
     :ex c-string :end :byte]
    (partial multprices [:trade])
    (partial divprices [:trade])))

(def tickempty
  (compile-frame
    []
    {}
    {}))

(defcodec- tick 
  (header ticktype
          {:enddata tickempty :endtick tickempty :startdata tickempty :version tickempty
           :tickask tickask :tickbid tickbid :tickfull tickfull
           :tickquote tickquote :ticktrade ticktrade}
          :ticktype))

(defcodec tik-codec
  [:header tick-header :ticks (repeated tick :prefix :none)])

(defn tik-file [file]
  (let [size (.length (clojure.java.io/file file))
        buffer (byte-array size)
        stream (clojure.java.io/input-stream file)
        _ (.read stream buffer)]
    (apply hash-map (decode tik-codec buffer))))
