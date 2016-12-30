(defproject asphalt "0.5.0-SNAPSHOT"
  :description "A Clojure library for JDBC access"
  :url "https://github.com/kumarshantanu/asphalt"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :java-source-paths ["java-src"]
  :javac-options ["-target" "1.7" "-source" "1.7" "-Xlint:-options"]
  :profiles {:dev {:dependencies [[org.clojure/tools.nrepl "0.2.10"]
                                  [clj-dbcp "0.8.1"]
                                  [simple-jndi "0.11.4.1"]
                                  [com.h2database/h2 "1.3.176"]]}
             :provided {:dependencies [[org.clojure/clojure "1.6.0"]]}
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
                    :test-paths ["perf"]}}
  :jvm-opts ^:replace ["-server" "-Xms2048m" "-Xmx2048m" #_FIXME "-Duser.timezone=UTC"]
  :global-vars {*warn-on-reflection* true
                *assert* true})
