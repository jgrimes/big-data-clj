(ns big-data.core
  (:import [com.backtype.hadoop.pail
            Pail PailSpec
            PailStructure]
           [java.io
            ByteArrayOutputStream ByteArrayInputStream
            DataOutputStream DataInputStream
            IOException]
           [java.lang RuntimeException]
           [java.util Collections]))


(defrecord Login [user-name #^long login-unix-time])

(defrecord LoginPailStructure []
  PailStructure
  (getType [this]
    Login)
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
          (throw (RuntimeException. e))))))
  (getTarget [this object]
    Collections/EMPTY_LIST)
  (isValidTarget [this dirs]
    true))

(defn write-logins []
  (let [login-pail (Pail/create "/tmp/logins" (LoginPailStructure.))
        out (.openWrite login-pail)]
    (doto out
      (.writeObject (Login. "alex" 1352679231))
      (.writeObject (Login. "bob" 1352674216))
      (.close))))

(defn read-logins []
  (let [login-pail (Pail. "/tmp/logins")]
    (map #(println (.userName %)) login-pail)))

(defn simple-pail-example []
  (let [pail (Pail/create "/tmp/mypail")
        os (.openWrite pail)]
    (doto os
      (.writeObject (byte-array [1 2 3]))
      (.writeObject (byte-array [1 2 3 4]))
      (.writeObject (byte-array [1 2 3 4 5]))
      (.close))))
