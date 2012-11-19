(ns lucre.datatests
  (:use [lucre.data]
        [midje.sweet]))

(def tl-testfile "test/data/tradelink/AAPL20070724.TIK")

(fact "Open Tradelink .TIK file" 
  (let [data (tik-file tl-testfile) 
        header (:header data) 
        ticks (:ticks data)] 
    data => truthy
    (:symbol header) => "AAPL"
    (> (count ticks) 0) => true
    (:trade (first ticks)) => 138.88M
    (:date (first ticks)) => 20070724))
