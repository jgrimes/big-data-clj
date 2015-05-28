(ns big-data.core
  (:import [com.backtype.hadoop.pail
            Pail PailSpec
            PailStructure
            SequenceFileFormat]
           [java.io
            ByteArrayOutputStream ByteArrayInputStream
            DataOutputStream DataInputStream
            IOException]
           [java.lang RuntimeException]
           [java.util Collections]))


(alter-var-root (var *compile-path*) (constantly "classes"))

(compile 'big_data.LoginPailStructure)
(compile 'big_data.PartitionedLoginPailStructure)
(import 'big_data.LoginPailStructure)
(import 'big_data.PartitionedLoginPailStructure)
(import big_data.LoginPailStructure.Login)

(defn write-logins []
  (let [login-pail (Pail/create "/tmp/logins" (LoginPailStructure.))
        out (.openWrite login-pail)]
    (doto out
      (.writeObject (Login. "alex" 1352679231))
      (.writeObject (Login. "bob" 1352674216))
      (.close))))

(defn read-logins []
  (let [login-pail (Pail. "/tmp/logins")]
    (map #(println (.user-name %)) login-pail)))

(defn read-logins-path [path]
  (let [login-pail (Pail. path)]
    (map #(println (.user-name %)) login-pail)))

(defn append-data []
  (let [login-pail (Pail. "/tmp/logins")
        update-pail (Pail. "/tmp/updates")]
    (doto login-pail
      (.absorb update-pail)
      (.consolidate))))

(defn create-compressed-pail []
  (let [options (java.util.HashMap.)]
    (do
      (doto options
        (.put SequenceFileFormat/CODEC_ARG SequenceFileFormat/CODEC_ARG_GZIP)
        (.put SequenceFileFormat/TYPE_ARG SequenceFileFormat/TYPE_ARG_BLOCK)
        )
      (Pail/create "/tmp/compressed" (PailSpec.
                                      "SequenceFile"
                                      options
                                      (LoginPailStructure. ))))))

(defn partition-data []
  (let [pail (Pail/create "/tmp/partitioned_logins"
                          (PartitionedLoginPailStructure.))
        os (.openWrite pail)]
    (doto os
      (.writeObject (Login. "chris" 1352702020))
      (.writeObject (Login. "david" 1352788472))
      (.close))))

(defn simple-pail-example []
  (let [pail (Pail/create "/tmp/mypail")
        os (.openWrite pail)]
    (doto os
      (.writeObject (byte-array [1 2 3]))
      (.writeObject (byte-array [1 2 3 4]))
      (.writeObject (byte-array [1 2 3 4 5]))
      (.close))))



