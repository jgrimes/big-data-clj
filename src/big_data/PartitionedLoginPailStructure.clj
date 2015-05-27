(ns big_data.PartitionedLoginPailStructure
  (:gen-class
   :extends big_data.LoginPailStructure)
  (:import [java.text SimpleDateFormat]
           [java.util Date]
           [java.text ParseException]
           [java.util ArrayList]))

(def formatter (SimpleDateFormat. "yyyy-MM-dd"))

(defn -getTarget [this object]
  (let [directory-path (ArrayList.)
        date (Date. (long (* 1000 (.login-unix-time object))))]
    (do
      (.add directory-path (.format formatter date))
      directory-path)))

(defn -isValidTarget [this strings]
  (println (map str strings))
  (if (not= 2 (count strings))
    (do
      (println "wrong1" strings)
      false)
    (try
      (do
        (println "wrong2")
        (not= nil (.parse formatter (aget strings 0))))
      (catch ParseException e
        false))))
