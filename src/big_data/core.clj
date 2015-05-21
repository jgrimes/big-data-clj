(ns big_data.core
  (:import [com.backtype.hadoop.pail
            Pail
            PailStructure]
           [java.io
            ByteArrayOutputStream ByteArrayInputStream
            DataOutputStream DataInputStream
            IOException]
           [java.lang RuntimeException]))


(defrecord Login [user-name #^long login-unix-time])

(defrecord LoginPailStructure []
  PailStructure
  (serialize [this login]
    (let [byte-out (ByteArrayOutputStream.)
          data-out (DataOutputStream. byte-out)
          user-bytes (.getBytes (.user-name login))]
      (try
        (doto data-out
          (.writeInt (alength user-bytes))
          (.write user-bytes)
          (.writeLong (.login-unix-time login))
          (.close))
        (catch IOException e
          (throw (RuntimeException. e))))
      (.toByteArray byte-out)))
  (deserialize [this serialized]
    (let [data-in (DataInputStream. (ByteArrayInputStream. serialized))]
      (try
        (let [user-bytes (byte-array (.readInt data-in))]
          (.read data-in user-bytes)
          (Login. (String. user-bytes) (.readLong data-in)))
        (catch IOException e
          (throw (RuntimeException. e)))))))

(defn simple-pail-example []
  (let [pail (Pail/create "/tmp/mypail")
        os (.openWrite pail)]
    (doto os
      (.writeObject (byte-array [1 2 3]))
      (.writeObject (byte-array [1 2 3 4]))
      (.writeObject (byte-array [1 2 3 4 5]))
      (.close))))
