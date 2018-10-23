(ns d2q-examples.datascript
  "d2q example with a DataScript data source."
  (:require [d2q.api :as d2q]
            [manifold.deferred :as mfd]
            [datascript.core :as dt]
            [vvvvalvalval.supdate.api :as supd]
            [d2q.helpers.resolvers :as d2q-res]
            [d2q-examples.api-schema :as schema]))

;; ------------------------------------------------------------------------------
;; Database

(def schema
  {:myblog.post/id {:db/unique :db.unique/identity}
   :myblog.post/author {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   :myblog.comment/id {:db/unique :db.unique/identity}
   :myblog.comment/author {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   :myblog.comment/about {:db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   :myblog.user/id {:db/unique :db.unique/identity}})

(def data
  [;; Users
   {:myblog.user/id "alice-hacker"
    :myblog.user/first-name "Alice"
    :myblog.user/last-name "P. Hacker"
    :myblog.entity/type :myblog.entity-types/user}
   {:myblog.user/id "john-doe"
    :myblog.user/first-name "John"
    :myblog.user/last-name "Doe"
    :myblog.entity/type :myblog.entity-types/user}

   ;; Posts and comments
   {:myblog.post/id "why-french-bread-is-the-best"
    :myblog.post/title "Why French Bread is the best"
    :myblog.post/content "It's the ingredients, stupid!"
    :myblog.post/author {:myblog.user/id "alice-hacker"}
    :myblog.post/published-at #inst"2018-10-10T08:07:31.119-00:00"
    :myblog.entity/type :myblog.entity-types/post
    :myblog.comment/_about
    {:myblog.comment/id #uuid"ef72b664-c389-4be6-9a9e-0733833eddd5"
     :myblog.comment/author {:myblog.user/id "john-doe"}
     :myblog.comment/content "Come on, American bread is not that bad!"
     :myblog.comment/published-at #inst"2018-10-10T08:12:28.243-00:00"
     :myblog.entity/type :myblog.entity-types/comment
     :myblog.comment/_about
     {:myblog.comment/id #uuid"234ea95b-5557-4c9a-a6ba-bcaa698a394a"
      :myblog.comment/author {:myblog.user/id "alice-hacker"}
      :myblog.comment/content "You know nothing, John Doe!"
      :myblog.comment/published-at #inst"2018-10-10T08:13:17.077-00:00"
      :myblog.entity/type :myblog.entity-types/comment
      }}}])

(def ctx
  {:db (dt/db-with (dt/empty-db schema) data)})

;; ------------------------------------------------------------------------------
;; Helpers for implementing resolvers

(defn resolver-for-scalar-datascript-attributes
  [entity-type]
  (fn [{:as qctx, :keys [db]} i+fcalls j+entities]
    (mfd/future
      {:d2q-res-cells
       (->> (dt/q '[:find ?j ?i ?v
                    :in $ ?entity-type [[?i ?attr]] [[?j ?ent]] :where
                    [?ent :myblog.entity/type ?entity-type]
                    [?ent ?attr ?v]]
              db
              entity-type
              (->> i+fcalls
                (map (fn [[i fcall]]
                       [i (:d2q-fcall-field fcall)])))
              (->> j+entities
                (map (fn [[j entity]]
                       [j (:db/id entity)]))))
         (mapv (fn [[j i v]]
                 (d2q/result-cell j i v))))})))

(defn resolver-for-entity-by-id-attribute
  "Given:
  - id-attr: the name of a DataScript attribute which is presumable :db.unique/identity
  - human-entity-type-name: a String, a human-readable name of the underlying entity type ('User', 'Blog Post', etc.), used in error-reporting.
  creates a d2q Resolver which finds Entities by their id-attr, returning DataScript Entities.

  In Field Calls, :d2q-fcall-arg is expected to be of the form {id-attr <<value>>}, e.g {:user/id \"my-user-id\"}.

  Errors will be returned for Field Calls for which there is no entity found."
  [id-attr human-entity-type-name]
  (d2q-res/entities-independent-resolver
    (fn [{:as qctx, :keys [db]} i+fcalls]
      (mfd/future
        (d2q-res/into-resolver-result
          (map
            (fn [[i fcall]]
              (let [ent-id (-> fcall :d2q-fcall-arg (get id-attr))]
                (if (nil? ent-id)
                  (ex-info (str "Missing " (pr-str id-attr) " in :d2q-fcall-arg")
                    {:myblog.errors/error-type :myblog.error-types/invalid-fcall-arg
                     :d2q-fcall-i i})
                  (let [entity-or-nil (dt/entity db [id-attr ent-id])]
                    (if (nil? entity-or-nil)
                      (ex-info (str "No " human-entity-type-name " with " (pr-str id-attr) " : " (pr-str ent-id))
                        {:myblog.errors/error-type :myblog.error-types/entity-not-found
                         :d2q-fcall-i i
                         id-attr ent-id})
                      (d2q/result-cell -1 i entity-or-nil)))))))
          i+fcalls)))))

(defn resolver-for-ref-one-datascript-attributes
  []
  (fn [qctx i+fcalls j+entities]
    (mfd/future
      {:d2q-res-cells
       (vec
         (for [[j ent] j+entities
               [i fcall] i+fcalls
               :let [field-name (:d2q-fcall-field fcall)
                     target-ent (get ent field-name)]
               :when (some? target-ent)]
           (d2q/result-cell j i target-ent)))})))

(defn resolve-from-simple-fns
  "A Resolver for Fields which can be computed simply by a function of the Entity and the :d2q-fcall-arg.
  Each Field computed by this Resolver must have a :myblog.simple-fn-field/compute key in its metadata,
  which must be a function with signature ([entity arg MISSING] -> (value | MISSING)).
  The function must return either the computed value, or its MISSING argument,
  which indicates that no result-cell should be returned for this Entity and Field Call."
  [_qctx i+fcall+metas j+entities]
  (mfd/future
    (let [MISSING (Object.)]
      (d2q-res/into-resolver-result
        (for [[j ent] j+entities
              [i fcall {:as field-meta, f :myblog.simple-fn-field/compute}] i+fcall+metas
              :let [result-cell-or-ex
                    (try
                      (let [v (f ent (:d2q-fcall-arg fcall) MISSING)]
                        (if (identical? v MISSING)
                          nil
                          (d2q/result-cell j i v)))
                      (catch Throwable err
                        (ex-info
                          (str "Error when computing Field " (pr-str :d2q-fcall-field fcall))
                          {:d2q-fcall-i i
                           :d2q-entcell-j j}
                          err)))]
              :when (some? result-cell-or-ex)]
          result-cell-or-ex)))))

;; ------------------------------------------------------------------------------
;; Server

(def decreasing
  "a comparator for sorting in decreasing order, the opposite of clojure.core/compare"
  (comp - compare))

(defn resolve-last-publication-comments
  [qctx i+fields j+entities]
  (mfd/future
    (d2q-res/into-resolver-result
      (->> i+fields
        (mapcat
          (fn [[i fcall]]
            (let [n (-> fcall :d2q-fcall-arg :n)]
              (if-not (or (nil? n) (and (integer? n) (not (neg? n))))
                [(ex-info
                   (str (pr-str :n) " must be either nil or a non-negative integer, got: " (pr-str n))
                   {:d2q-fcall-i i})]
                (->> j+entities
                  (map
                    (fn [[j publication]]
                      (let [sorted-comments (->> publication :myblog.comment/_about
                                              (sort-by :myblog.comment/published-at decreasing))]
                        (d2q/result-cell
                          j i
                          (if (some? n)
                            (vec (take n sorted-comments))
                            sorted-comments))))))))))
        vec))))

(defn user-full-name
  [user _arg MISSING]
  (let [first-name (:myblog.user/first-name user)
        last-name (:myblog.user/last-name user)]
    (if (and first-name last-name)
      (str first-name " " last-name)
      MISSING)))

(defn resolve-user-n-posts
  [{:as qctx, :keys [db]} i+fields j+entities]
  (mfd/future
    {:d2q-res-cells
     (let [j->n-posts
           (into {}
             (dt/q '[:find ?j (count ?post)
                     :in $ [[?j ?user]] :where
                     [?post :myblog.post/author ?user]]
               db
               (->> j+entities
                 (mapv (fn [[j user]]
                         [j (:db/id user)])))))]
       (vec
         (for [[j _] j+entities
               [i _ _] i+fields]
           (let [n-posts (get j->n-posts j 0)]
             (d2q/result-cell j i n-posts)))))}))

(def fields
  (->> schema/fields
    ))


(def server
  (d2q/server
    {:d2q.server/fields fields
     :d2q.server/resolvers
     [{:d2q.resolver/name :myblog.resolvers/user-by-id
       :d2q.resolver/field->meta {:myblog/user-by-id nil}
       :d2q.resolver/compute (resolver-for-entity-by-id-attribute :myblog.user/id "User")}
      {:d2q.resolver/name :myblog.resolvers/user-fields
       :d2q.resolver/field->meta {:myblog.user/first-name nil,
                                  :myblog.user/id nil,
                                  :myblog.user/last-name nil}
       :d2q.resolver/compute (resolver-for-scalar-datascript-attributes :myblog.entity-types/user)}
      {:d2q.resolver/name :myblog.resolvers/post-by-id
       :d2q.resolver/field->meta {:myblog/post-by-id nil}
       :d2q.resolver/compute (resolver-for-entity-by-id-attribute :myblog.post/id "Blog Post")}
      {:d2q.resolver/name :myblog.resolvers/post-fields
       :d2q.resolver/field->meta {:myblog.post/content nil,
                                  :myblog.post/id nil,
                                  :myblog.post/published-at nil,
                                  :myblog.post/title nil}
       :d2q.resolver/compute (resolver-for-scalar-datascript-attributes :myblog.entity-types/post)}
      {:d2q.resolver/name :myblog.resolvers/post-author
       :d2q.resolver/field->meta {:myblog.post/author nil}
       :d2q.resolver/compute (resolver-for-ref-one-datascript-attributes)}
      {:d2q.resolver/name :myblog.resolvers/comment-by-id
       :d2q.resolver/field->meta {:myblog/comment-by-id nil}
       :d2q.resolver/compute (resolver-for-entity-by-id-attribute :myblog.comment/id "Blog Comment")}
      {:d2q.resolver/name :myblog.resolvers/comment-fields
       :d2q.resolver/field->meta {:myblog.comment/content nil,
                                  :myblog.comment/id nil,
                                  :myblog.comment/published-at nil}
       :d2q.resolver/compute (resolver-for-scalar-datascript-attributes :myblog.entity-types/comment)}
      {:d2q.resolver/name :myblog.resolvers/comment-author
       :d2q.resolver/field->meta {:myblog.comment/author nil
                                  :myblog.comment/about nil}
       :d2q.resolver/compute (resolver-for-ref-one-datascript-attributes)}
      {:d2q.resolver/name :myblog.resolvers/publication-comments
       :d2q.resolver/field->meta {:myblog.publication/last-n-comments nil}
       :d2q.resolver/compute #'resolve-last-publication-comments}
      {:d2q.resolver/name :myblog.resolvers/resolve-by-simple-fns
       :d2q.resolver/field->meta {:myblog.user/full-name {:myblog.simple-fn-field/compute #'user-full-name}}
       :d2q.resolver/compute #'resolve-from-simple-fns}
      {:d2q.resolver/name :myblog.resolvers/user-n-posts
       :d2q.resolver/field->meta {:myblog.user/n-posts nil}
       :d2q.resolver/compute #'resolve-user-n-posts}]}))

;; ------------------------------------------------------------------------------
;; Manual tests

(comment
  (->
    @(d2q/query server ctx
       [{:d2q-fcall-field :myblog/post-by-id
         :d2q-fcall-key "p1"
         :d2q-fcall-arg {:myblog.post/id "why-french-bread-is-the-best"}
         :d2q-fcall-subquery [:myblog.post/title
                              {:d2q-fcall-field :myblog.post/author
                               :d2q-fcall-subquery
                               [:myblog.user/id
                                :myblog.user/first-name
                                :myblog.user/last-name
                                :myblog.user/full-name
                                :myblog.user/n-posts]}
                              {:d2q-fcall-field :myblog.publication/last-n-comments
                               :d2q-fcall-key :last-5-comments
                               :d2q-fcall-arg {:n 5}
                               :d2q-fcall-subquery [:myblog.comment/id
                                                    :myblog.comment/content
                                                    :myblog.comment/published-at
                                                    {:d2q-fcall-field :myblog.comment/about
                                                     :d2q-fcall-subquery [:myblog.post/id
                                                                          :myblog.comment/id]}]}]}
        {:d2q-fcall-field :myblog/user-by-id
         :d2q-fcall-key "no-such-user"
         :d2q-fcall-arg {:myblog.user/id "USER-ID-THAT-DOES-NOT-EXIST"}
         :d2q-fcall-subquery [:myblog.user/id
                              :myblog.user/first-name
                              :myblog.user/last-name]}
        {:d2q-fcall-field :myblog/user-by-id
         :d2q-fcall-key "invalid-fcall"
         :d2q-fcall-subquery [:myblog.user/id
                              :myblog.user/first-name
                              :myblog.user/last-name]}]
       [nil])
    (supd/supdate {:d2q-errors [Throwable->map]}))
  =>
  '{:d2q-results [{"p1" {:myblog.post/title "Why French Bread is the best",
                         :myblog.post/author {:myblog.user/id "alice-hacker",
                                              :myblog.user/first-name "Alice",
                                              :myblog.user/last-name "P. Hacker",
                                              :myblog.user/full-name "Alice P. Hacker",
                                              :myblog.user/n-posts 1},
                         :last-5-comments [{:myblog.comment/id #uuid"ef72b664-c389-4be6-9a9e-0733833eddd5",
                                            :myblog.comment/published-at #inst"2018-10-10T08:12:28.243-00:00",
                                            :myblog.comment/content "Come on, American bread is not that bad!",
                                            :myblog.comment/about {:myblog.post/id "why-french-bread-is-the-best"}}]}}],
    :d2q-errors ({:cause "No User with :myblog.user/id : \"USER-ID-THAT-DOES-NOT-EXIST\"",
                  :via [{:type clojure.lang.ExceptionInfo,
                         :message "Error in d2q Resolver :myblog.resolvers/user-by-id, on Field :myblog/user-by-id at key \"no-such-user\"",
                         :at [clojure.core$ex_info invokeStatic "core.clj" 4617],
                         :data {:d2q.error/type :d2q.error.type/resolver,
                                :d2q.resolver/name :myblog.resolvers/user-by-id,
                                :d2q.error/query-trace ([:d2q.trace.op/resolver
                                                         {:d2q.resolver/name :myblog.resolvers/user-by-id}]
                                                         [:d2q.trace.op/query
                                                          #d2q.datatypes.Query{:d2q-query-id nil,
                                                                               :d2q-query-fcalls [:...elided],
                                                                               :d2q-rev-query-path ()}]),
                                :d2q.error/field-call #d2q.datatypes.FieldCall{:d2q-fcall-field :myblog/user-by-id,
                                                                               :d2q-fcall-key "no-such-user",
                                                                               :d2q-fcall-arg {:myblog.user/id "USER-ID-THAT-DOES-NOT-EXIST"},
                                                                               :d2q-fcall-subquery #d2q.datatypes.Query{:d2q-query-id nil,
                                                                                                                        :d2q-query-fcalls [#d2q.datatypes.FieldCall{:d2q-fcall-field :myblog.user/id,
                                                                                                                                                                    :d2q-fcall-key :myblog.user/id,
                                                                                                                                                                    :d2q-fcall-arg nil,
                                                                                                                                                                    :d2q-fcall-subquery nil,
                                                                                                                                                                    :d2q-fcall-rev-query-path (0
                                                                                                                                                                                                :d2q-fcall-subquery
                                                                                                                                                                                                1)}
                                                                                                                                           #d2q.datatypes.FieldCall{:d2q-fcall-field :myblog.user/first-name,
                                                                                                                                                                    :d2q-fcall-key :myblog.user/first-name,
                                                                                                                                                                    :d2q-fcall-arg nil,
                                                                                                                                                                    :d2q-fcall-subquery nil,
                                                                                                                                                                    :d2q-fcall-rev-query-path (1
                                                                                                                                                                                                :d2q-fcall-subquery
                                                                                                                                                                                                1)}
                                                                                                                                           #d2q.datatypes.FieldCall{:d2q-fcall-field :myblog.user/last-name,
                                                                                                                                                                    :d2q-fcall-key :myblog.user/last-name,
                                                                                                                                                                    :d2q-fcall-arg nil,
                                                                                                                                                                    :d2q-fcall-subquery nil,
                                                                                                                                                                    :d2q-fcall-rev-query-path (2
                                                                                                                                                                                                :d2q-fcall-subquery
                                                                                                                                                                                                1)}],
                                                                                                                        :d2q-rev-query-path (:d2q-fcall-subquery
                                                                                                                                              1)},
                                                                               :d2q-fcall-rev-query-path (1)}}}
                        {:type clojure.lang.ExceptionInfo,
                         :message "No User with :myblog.user/id : \"USER-ID-THAT-DOES-NOT-EXIST\"",
                         :at [clojure.core$ex_info invokeStatic "core.clj" 4617],
                         :data {:myblog.errors/error-type :myblog.error-types/entity-not-found,
                                :d2q-fcall-i 0,
                                :myblog.user/id "USER-ID-THAT-DOES-NOT-EXIST"}}],
                  :trace [[clojure.core$ex_info invokeStatic "core.clj" 4617]
                          [clojure.core$ex_info invoke "core.clj" 4617]
                          [d2q_examples.datascript$resolver_for_entity_by_id_attribute$fn__28989$f__20334__auto____28991$fn__28993
                           invoke
                           "datascript.clj"
                           100]
                          [clojure.core$map$fn__4781$fn__4782 invoke "core.clj" 2633]
                          [clojure.lang.PersistentVector reduce "PersistentVector.java" 341]
                          [clojure.core$transduce invokeStatic "core.clj" 6600]
                          [clojure.core$into invokeStatic "core.clj" 6614]
                          [clojure.core$into invoke "core.clj" 6604]
                          [d2q.helpers.resolvers$into_resolver_result invokeStatic "resolvers.clj" 19]
                          [d2q.helpers.resolvers$into_resolver_result invoke "resolvers.clj" 8]
                          [d2q_examples.datascript$resolver_for_entity_by_id_attribute$fn__28989$f__20334__auto____28991
                           invoke
                           "datascript.clj"
                           90]
                          [clojure.lang.AFn run "AFn.java" 22]
                          [io.aleph.dirigiste.Executor$3 run "Executor.java" 318]
                          [io.aleph.dirigiste.Executor$Worker$1 run "Executor.java" 62]
                          [manifold.executor$thread_factory$reify__20216$f__20217 invoke "executor.clj" 44]
                          [clojure.lang.AFn run "AFn.java" 22]
                          [java.lang.Thread run "Thread.java" 745]],
                  :data {:myblog.errors/error-type :myblog.error-types/entity-not-found,
                         :d2q-fcall-i 0,
                         :myblog.user/id "USER-ID-THAT-DOES-NOT-EXIST"}}
                  {:cause "Missing :myblog.user/id in :d2q-fcall-arg",
                   :via [{:type clojure.lang.ExceptionInfo,
                          :message "Error in d2q Resolver :myblog.resolvers/user-by-id, on Field :myblog/user-by-id at key \"invalid-fcall\"",
                          :at [clojure.core$ex_info invokeStatic "core.clj" 4617],
                          :data {:d2q.error/type :d2q.error.type/resolver,
                                 :d2q.resolver/name :myblog.resolvers/user-by-id,
                                 :d2q.error/query-trace ([:d2q.trace.op/resolver
                                                          {:d2q.resolver/name :myblog.resolvers/user-by-id}]
                                                          [:d2q.trace.op/query
                                                           #d2q.datatypes.Query{:d2q-query-id nil,
                                                                                :d2q-query-fcalls [:...elided],
                                                                                :d2q-rev-query-path ()}]),
                                 :d2q.error/field-call #d2q.datatypes.FieldCall{:d2q-fcall-field :myblog/user-by-id,
                                                                                :d2q-fcall-key "invalid-fcall",
                                                                                :d2q-fcall-arg nil,
                                                                                :d2q-fcall-subquery #d2q.datatypes.Query{:d2q-query-id nil,
                                                                                                                         :d2q-query-fcalls [#d2q.datatypes.FieldCall{:d2q-fcall-field :myblog.user/id,
                                                                                                                                                                     :d2q-fcall-key :myblog.user/id,
                                                                                                                                                                     :d2q-fcall-arg nil,
                                                                                                                                                                     :d2q-fcall-subquery nil,
                                                                                                                                                                     :d2q-fcall-rev-query-path (0
                                                                                                                                                                                                 :d2q-fcall-subquery
                                                                                                                                                                                                 2)}
                                                                                                                                            #d2q.datatypes.FieldCall{:d2q-fcall-field :myblog.user/first-name,
                                                                                                                                                                     :d2q-fcall-key :myblog.user/first-name,
                                                                                                                                                                     :d2q-fcall-arg nil,
                                                                                                                                                                     :d2q-fcall-subquery nil,
                                                                                                                                                                     :d2q-fcall-rev-query-path (1
                                                                                                                                                                                                 :d2q-fcall-subquery
                                                                                                                                                                                                 2)}
                                                                                                                                            #d2q.datatypes.FieldCall{:d2q-fcall-field :myblog.user/last-name,
                                                                                                                                                                     :d2q-fcall-key :myblog.user/last-name,
                                                                                                                                                                     :d2q-fcall-arg nil,
                                                                                                                                                                     :d2q-fcall-subquery nil,
                                                                                                                                                                     :d2q-fcall-rev-query-path (2
                                                                                                                                                                                                 :d2q-fcall-subquery
                                                                                                                                                                                                 2)}],
                                                                                                                         :d2q-rev-query-path (:d2q-fcall-subquery
                                                                                                                                               2)},
                                                                                :d2q-fcall-rev-query-path (2)}}}
                         {:type clojure.lang.ExceptionInfo,
                          :message "Missing :myblog.user/id in :d2q-fcall-arg",
                          :at [clojure.core$ex_info invokeStatic "core.clj" 4617],
                          :data {:myblog.errors/error-type :myblog.error-types/invalid-fcall-arg, :d2q-fcall-i 1}}],
                   :trace [[clojure.core$ex_info invokeStatic "core.clj" 4617]
                           [clojure.core$ex_info invoke "core.clj" 4617]
                           [d2q_examples.datascript$resolver_for_entity_by_id_attribute$fn__28989$f__20334__auto____28991$fn__28993
                            invoke
                            "datascript.clj"
                            95]
                           [clojure.core$map$fn__4781$fn__4782 invoke "core.clj" 2633]
                           [clojure.lang.PersistentVector reduce "PersistentVector.java" 341]
                           [clojure.core$transduce invokeStatic "core.clj" 6600]
                           [clojure.core$into invokeStatic "core.clj" 6614]
                           [clojure.core$into invoke "core.clj" 6604]
                           [d2q.helpers.resolvers$into_resolver_result invokeStatic "resolvers.clj" 19]
                           [d2q.helpers.resolvers$into_resolver_result invoke "resolvers.clj" 8]
                           [d2q_examples.datascript$resolver_for_entity_by_id_attribute$fn__28989$f__20334__auto____28991
                            invoke
                            "datascript.clj"
                            90]
                           [clojure.lang.AFn run "AFn.java" 22]
                           [io.aleph.dirigiste.Executor$3 run "Executor.java" 318]
                           [io.aleph.dirigiste.Executor$Worker$1 run "Executor.java" 62]
                           [manifold.executor$thread_factory$reify__20216$f__20217 invoke "executor.clj" 44]
                           [clojure.lang.AFn run "AFn.java" 22]
                           [java.lang.Thread run "Thread.java" 745]],
                   :data {:myblog.errors/error-type :myblog.error-types/invalid-fcall-arg, :d2q-fcall-i 1}})}


  )
