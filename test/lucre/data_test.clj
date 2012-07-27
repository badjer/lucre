(ns lucre.data-test
  (:use clojure.test
        lucre.data))

;(def testfile "test/data/2012.Last.ntd")

;(deftest ntd-file-test
  ;(testing "Open NinjaTrader .ntd file"
    ;(let [file (ntd-file testfile)]
      ;(is (not (nil? file)))
      ;(is (= {:sym "AAPL"} (first file))))))

(def tl-testfile "test/data/tradelink/AAPL20070724.TIK")

(deftest tik-file-test
  (testing "Open Tradelink .TIK file"
    (let [data (tik-file tl-testfile)
          header (:header data)
          ticks (:ticks data)]
      (is (not (nil? data)))
      (is (= "AAPL" (:symbol header)))
      (is (> (count ticks) 0))
      (is (= 138.88M (:trade (first ticks))))
      (is (= 20070724 (:date (first ticks)))))))
