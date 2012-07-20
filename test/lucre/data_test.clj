(ns lucre.data-test
  (:use clojure.test
        lucre.data))

(def testfile "test/data/2012.Last.ntd")

(deftest ntd-file-test
  (testing "Open NinjaTrader .ntd file"
    (let [file (ntd-file testfile)]
      (is (not (nil? file)))
      (is (= {:sym "AAPL"} (first file))))))
