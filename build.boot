(set-env!
 :source-paths #{"src" "gen-java"}
 :dependencies '[[thrift-clj "0.2.1"]
                 [ch.qos.logback/logback-classic "1.1.1"]])



(deftask build
  []
  (comp
   (javac)))





