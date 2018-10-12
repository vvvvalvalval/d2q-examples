(defproject d2q-examples "0.1.0-SNAPSHOT"
  :description "Examples of using d2q in combination with various other libraries"
  :url "http://example.com/FIXME"
  :license {:name "MIT license"
            :url "https://opensource.org/licenses/MIT"}
  :profiles
  {:dev
   {:lein-tools-deps/config {:aliases [:dev]}
    :repl-options {:nrepl-middleware [sc.nrepl.middleware/wrap-letsc]}}}

  :plugins [[lein-tools-deps "0.4.1"]]
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files [:install :user :project]})
