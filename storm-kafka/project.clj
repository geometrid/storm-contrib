(defproject storm/storm-kafka "0.9.0-wip21-ics"
  :java-source-paths ["src/jvm"]
  :plugins [[s3-wagon-private "1.1.2"]]
  :repositories {"scala-tools" "http://scala-tools.org/repo-releases"
                 "conjars" "http://conjars.org/repo/"
		 "releases" {:url "s3p://artifacts.chimpy.us/maven-s3p/releases/" :creds :gpg}
                 "snapshots" {:url "s3p://artifacts.chimpy.us/maven-s3p/snapshots/" :creds :gpg}}
  :dependencies [[org.scala-lang/scala-library "2.9.2"]
                  [com.twitter/kafka_2.9.2 "0.7.0"
                  :exclusions [org.apache.zookeeper/zookeeper
                               log4j/log4j]]]
  :profiles
  {:provided {:dependencies [[storm/storm-core "0.9.0-wip21-ics"]
                             [org.slf4j/log4j-over-slf4j "1.6.6"]
                             ;;[ch.qos.logback/logback-classic "1.0.6"]
                             [org.clojure/clojure "1.4.0"]]}}
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
  :min-lein-version "2.0")
