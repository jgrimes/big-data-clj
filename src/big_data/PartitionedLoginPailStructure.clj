(ns big_data.PartitionedLoginPailStructure
  (:gen-class
   :extends big_data.LoginPailStructure)
  (:import [java.text SimpleDateFormat]
           [java.util Date]
           [java.text ParseException]))

(def formatter (SimpleDateFormat. "yyyy-MM-dd"))

(defn -getTarget [this object]
  (let [directory-path []
        date (Date. (* 1000 (.loginUnixTime object)))]
    (do
      (.add directory-path (.format formatter date))
      directory-path)))

(defn -isValidTarget [this strings]
  (if (not= 1 (.length strings))
    false
    (try
      (.parse formatter (not= nil (nth strings 0)))
      (catch ParseException e
        false))))
