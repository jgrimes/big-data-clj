(ns big_data.core
  (:import [com.backtype.hadoop.pail Pail]))



(defn simple-pail-example []
  (let [pail (Pail/create "/tmp/mypail")
        os (.openWrite pail)]
    (doto os
      (.writeObject (byte-array [1 2 3]))
      (.writeObject (byte-array [1 2 3 4]))
      (.writeObject (byte-array [1 2 3 4 5]))
      (.close))))
