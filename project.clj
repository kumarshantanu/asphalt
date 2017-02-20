(defproject asphalt "0.6.0"
  :description "A Clojure library for JDBC access"
  :url "https://github.com/kumarshantanu/asphalt"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :java-source-paths ["java-src"]
  :javac-options ["-target" "1.7" "-source" "1.7" "-Xlint:-options"]
  :profiles {:dev {:dependencies [[org.clojure/tools.nrepl "0.2.10"]
                                  [simple-jndi "0.11.4.1"]
                                  [com.h2database/h2 "1.3.176"]]}
             :provided {:dependencies [[org.clojure/clojure "1.6.0"]
                                       [clj-dbcp "0.8.2"]]
                        :test-paths ["test-connpool/dbcp"]}
             :c16 {:dependencies [[org.clojure/clojure "1.6.0"]]}
             :c17 {:dependencies [[org.clojure/clojure "1.7.0"]]
                   :global-vars {*unchecked-math* :warn-on-boxed}}
             :c18 {:dependencies [[org.clojure/clojure "1.8.0"]]
                   :global-vars {*unchecked-math* :warn-on-boxed}}
             :c19 {:dependencies [[org.clojure/clojure "1.9.0-alpha14"]]
                   :global-vars {*unchecked-math* :warn-on-boxed}}
             :dln {:jvm-opts ["-Dclojure.compiler.direct-linking=true"]}
             :perf {:dependencies [[citius "0.2.4"]
                                   [org.clojure/java.jdbc "0.7.0-alpha1"]]
                    :test-paths ["perf"]}
             :dbcp  {:test-paths ["test-connpool/dbcp"]  :dependencies [[clj-dbcp "0.8.2"]]}
             :dbcp2 {:test-paths ["test-connpool/dbcp2"] :dependencies [[clj-dbcp "0.9.0"]]}
             :c3p0  {:test-paths ["test-connpool/c3p0"]  :dependencies [[com.mchange/c3p0 "0.9.5.2"]]}
             :bone  {:test-paths ["test-connpool/bone"]  :dependencies [[com.jolbox/bonecp "0.8.0.RELEASE"]]}}
  :jvm-opts ^:replace ["-server" "-Xms2048m" "-Xmx2048m" #_FIXME "-Duser.timezone=UTC"]
  :global-vars {*warn-on-reflection* true
                *assert* true})
