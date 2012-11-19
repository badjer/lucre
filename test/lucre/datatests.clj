(ns lucre.datatests
  (:use [lucre.data]
        [midje.sweet]))

;(def testfile "test/data/2012.Last.ntd")

;(deftest ntd-file-test
  ;(testing "Open NinjaTrader .ntd file"
    ;(let [file (ntd-file testfile)]
      ;(is (not (nil? file)))
      ;(is (= {:sym "AAPL"} (first file))))))

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
