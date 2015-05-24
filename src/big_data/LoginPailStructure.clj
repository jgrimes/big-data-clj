(ns big_data.LoginPailStructure
  (:gen-class
   :implements [com.backtype.hadoop.pail.PailStructure])
  (:import
   [com.backtype.hadoop.pail
    Pail PailSpec
    PailStructure]
   [java.io
    ByteArrayOutputStream ByteArrayInputStream
    DataOutputStream DataInputStream
    IOException]
   [java.lang RuntimeException]
   [java.util Collections]))

(defrecord Login [user-name login-unix-time])

(defn -getType [this]
  Login)

(defn -isValidTarget [this dirs]
  true)

(defn -serialize [this login]
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

(defn -deserialize [this serialized]
  (let [data-in (DataInputStream. (ByteArrayInputStream. serialized))]
    (try
      (let [user-bytes (byte-array (.readInt data-in))]
        (.read data-in user-bytes)
        (Login. (String. user-bytes) (.readLong data-in)))
      (catch IOException e
        (throw (RuntimeException. e))))))

(defn -getTarget [this object]
  Collections/EMPTY_LIST)

