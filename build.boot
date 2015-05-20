(set-env!
 :source-paths #{"src" "gen-java"}
 :dependencies '[[thrift-clj "0.2.1"]
                 [ch.qos.logback/logback-classic "1.1.1"]
                 [org.apache.hadoop/hadoop-core "1.2.1"]
                 [com.backtype/dfs-datastores "1.3.6"]])

(require '[big-data.core])

(deftask build
  []
  (comp
   (javac)))

(deftask run
  []
  (comp
   (javac)
   (with-pre-wrap fileset
     (require 'thrift-clj.core)
     fileset)
   (repl)))
