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







; Working with Ninjatrader .ntd files
;   Doesn't work - their file format is undocumented and 
;   annoyingly complex. 
;   I'm switching over to Tradelink because it's open source

;(defn from-ms-ticks [ticks]
  ;"Converts a date in MS ticks into a java Date"
  ;(java.util.Date. (long (/ (- ticks 634200192000000000)
                            ;10000))))
;;
;(defn- decode-header [rec]
  ;(let [res (apply hash-map rec)]
    ;(assoc res :idate (from-ms-ticks (:iticks res)))))
;
;;(defn- vol-type [mask]
  ;(case (bit-and 112 mask)
    ;0 nil
    ;16 :ubyte
    ;48 :ubyte
    ;96 :uint16-be
    ;112 :uint32-be
    ;64 :uint64-le-be))
;
;(defn- price-type [mask]
  ;(case (bit-and 12 mask)
    ;0 nil
    ;4 :ubyte
    ;8 :uint16-be
    ;12 :uint32-be))
;
;(defn- time-type [mask]
  ;(case (bit-and 3 mask)
    ;0 nil
    ;1 :ubyte
    ;2 :uint16-be
    ;3 :uint24-be))
;
;(defn- adjust-delta [valtype x]
  ;(case valtype
    ;nil x
    ;:ubyte (- x 0x80)
    ;:uint16-be (- x 0x4000)
    ;:uint32-be (- x 0x40000000)))
;
;(defn- adjust-vol [mask x]
  ;(if (= (bit-and 112 mask) 48)
    ;(* x 100)
    ;x))
;
;(defn- adjust-price [mask x]
  ;(adjust-delta (price-type mask) x))
;
;(defn- adjust-time [mask x]
  ;(adjust-delta (time-type mask) x))
;
;(defn- decode-tick [mask rec]
  ;(let [res (apply hash-map rec)]
    ;(assoc res 
           ;:vol (adjust-vol mask (:vol res))
           ;:time-d (adjust-time mask (:time-d res)) 
           ;:price-d (adjust-price mask (:price-d res)))))
;
;(defn- get-body-codec- [mask]
  ;(let [voltype (vol-type mask)
        ;pricetype (price-type mask)
        ;timetype (time-type mask)]
    ;(gloss/compile-frame 
      ;[:time-d (or timetype :none)
       ;;:price-d (or pricetype :none)
       ;:vol (or voltype :none)]
      ;identity
      ;#(decode-tick mask %))))
;
;(defn get-body-frame-size- [mask]
  ;(let [types (map #(% mask) [vol-type price-type time-type])
        ;size (fn [t] (case t
                       ;nil 0
                       ;;:ubyte 1
                       ;:uint16-be 2
                       ;:uint24-be 3
                       ;:uint32-be 4
                       ;:uint64-le-be 8))]
    ;(apply + (map size types))))
;
;(def get-body-frame-size (memoize get-body-frame-size-))
;
;(def get-body-codec (memoize get-body-codec-))
;
;(defn get-tick [stream]
  ;(let [mask (.read stream)
        ;codec (get-body-codec mask)
        ;size (get-body-frame-size mask)
        ;buffer (byte-array size)
        ;_ (.read stream buffer)]
    ;(gloss.io/decode codec buffer)))
;

;(def ntd-header-codec (gloss/compile-frame 
                        ;[:pm :float64-le 
                         ;:junk1 :uint32-le 
                         ;:count :uint32-le 
                         ;:iprice :float64-le 
                         ;:iprice2 :float64-le 
                         ;:iprice3 :float64-le 
                         ;:iprice4 :float64-le 
                         ;:iticks :uint64-le-le 
                         ;:ivol :uint64-le-le]
                        ;identity
                        ;decode-header))
;
;(defn get-header [stream]
  ;(let [buffer (byte-array 64)
        ;_ (.read stream buffer)]
    ;(gloss.io/decode ntd-header-codec buffer)))
;
;(defn ntd-file [file]
  ;(let [stream (clojure.java.io/input-stream file)]
    ;; TODO: Parse the rest of the file as well
    ;(get-header stream)))
