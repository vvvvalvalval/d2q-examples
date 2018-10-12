(ns d2q-examples.sql
  (:require [clojure.java.jdbc :as jdbc]
            [manifold.deferred :as mfd]
            [d2q-examples.utils :as u]
            [clojure.string :as str]
            [d2q.api :as d2q]
            [d2q.helpers.resolvers :as d2q-res]
            [vvvvalvalval.supdate.api :as supd]
            [d2q-examples.api-schema :as schema])
  (:import (java.util UUID)))

(defn create-db
  [subname]
  {:classname "org.h2.Driver"
   :subprotocol "h2:mem"
   :subname (str subname ";DB_CLOSE_DELAY=-1")
   :user "sa"
   :password ""})

(defn clear-db!
  [conn]
  (jdbc/execute! conn ["DROP ALL OBJECTS;"]))

(defn create-tables!
  [conn]
  (doto conn
    (jdbc/execute!
      ["
  CREATE TABLE myblog_users (
    user_id VARCHAR PRIMARY KEY,
    user_first_name VARCHAR,
    user_last_name VARCHAR
  );

  CREATE TABLE myblog_posts (
    post_id VARCHAR PRIMARY KEY,
    post_title VARCHAR,
    post_content VARCHAR,
    post_author_id VARCHAR, FOREIGN KEY (post_author_id) REFERENCES myblog_users(user_id),
    post_published_at TIMESTAMP WITHOUT TIME ZONE
  );

  CREATE TABLE myblog_comments (
    comment_id UUID PRIMARY KEY,
    comment_author_id VARCHAR, FOREIGN KEY (comment_author_id) REFERENCES myblog_users(user_id),
    comment_about_post_id VARCHAR, FOREIGN KEY (comment_about_post_id) REFERENCES myblog_posts(post_id),
    comment_about_comment_id UUID, FOREIGN KEY (comment_about_comment_id) REFERENCES myblog_comments(comment_id),
    comment_content VARCHAR,
    comment_published_at TIMESTAMP WITHOUT TIME ZONE
  );

  CREATE VIEW myblog_users_enriched AS (
    SELECT
      *,
      CONCAT(user_first_name, ' ', user_last_name) AS user_full_name
    FROM myblog_users
  );
  "])))

(defn fill-tables!
  [conn]
  (doto conn
    (jdbc/insert-multi! "myblog_users"
      [{:user_id "alice-hacker"
        :user_first_name "Alice"
        :user_last_name "P. Hacker"}
       {:user_id "john-doe"
        :user_first_name "John"
        :user_last_name "Doe"}])
    (jdbc/insert-multi! "myblog_posts"
      [{:post_id "why-french-bread-is-the-best"
        :post_title "Why French Bread is the best"
        :post_content "It's the ingredients, stupid!"
        :post_author_id "alice-hacker"
        :post_published_at #inst"2018-10-10T08:07:31.119-00:00"}])
    (jdbc/insert-multi! "myblog_comments"
      [{:comment_id #uuid"ef72b664-c389-4be6-9a9e-0733833eddd5"
        :comment_author_id "john-doe"
        :comment_content "Come on, American bread is not that bad!"
        :comment_published_at #inst"2018-10-10T08:12:28.243-00:00"
        :comment_about_post_id "why-french-bread-is-the-best"
        :comment_about_comment_id nil}
       {:comment_id #uuid"234ea95b-5557-4c9a-a6ba-bcaa698a394a"
        :comment_author_id "alice-hacker"
        :comment_content "You know nothing, John Doe!"
        :comment_published_at #inst"2018-10-10T08:13:17.077-00:00"
        :comment_about_post_id nil
        :comment_about_comment_id #uuid"ef72b664-c389-4be6-9a9e-0733833eddd5"}])))

(defn create-ctx!
  [subname]
  {:conn (doto (create-db subname)
           clear-db!
           create-tables!
           fill-tables!)})

(def ctx (create-ctx! "myblog"))

(comment
  (jdbc/query (:conn ctx) ["SHOW TABLES;"])

  (jdbc/query (:conn ctx) ["SELECT * FROM myblog_users_enriched"])
  )

;; ------------------------------------------------------------------------------
;; Helpers for implementing resolvers

(defn resolver-finding-entity-by-primary-key
  [field-name table-name column-name human-readable-entity-type]
  {:pre [(keyword? field-name) (keyword? table-name) (keyword? column-name)]}
  (d2q-res/validating-fcall-args
    {:d2q.helpers.validating-fcall-args/checker-required? true}
    (d2q-res/entities-independent-resolver
      (fn [{:as qctx :keys [conn]} i+fcalls]
        (mfd/future
          (letfn []
            (let [ent-id->is (u/group-and-map-by
                               (fn [[_i fcall]]
                                 (-> fcall :d2q-fcall-arg field-name))
                               (fn [[i _fcall]] i)
                               i+fcalls)
                  q [(format "SELECT %1$s FROM %2$s WHERE %1$s = ANY (?)"
                       (name column-name) (name table-name))
                     (into-array String (keys ent-id->is))]
                  found-ent-ids (into #{}
                                  (map column-name)
                                  (jdbc/query conn q))
                  missing-ent-ids (->> ent-id->is keys (remove found-ent-ids))
                  not-found-errors
                  (for [ent-id missing-ent-ids
                        i (get ent-id->is ent-id)]
                    (ex-info
                      (str "No " human-readable-entity-type " with " (pr-str field-name) " : " (pr-str ent-id))
                      {:myblog.errors/error-type :myblog.error-types/entity-not-found
                       :d2q-fcall-i i
                       field-name ent-id}))]
              {:d2q-errors (vec not-found-errors)
               :d2q-res-cells (vec
                                (for [user-id found-ent-ids
                                      i (get ent-id->is user-id)]
                                  (d2q/result-cell -1 i
                                    {field-name user-id})))})))))))

(defn resolver-reading-table-rows
  [table-name id-attr primary-key-col]
  {:pre [(keyword? table-name) (keyword? id-attr) (keyword? primary-key-col)]}
  (d2q-res/validating-input-entities
    (fn check-contains-id-attr [ent]
      (when (and (map? ent) (contains? ent id-attr))
        (let [id (get ent id-attr)]
          (or
            (some? id)
            (throw
              (ex-info (str "Invalid " (pr-str id-attr) " : " (pr-str id)) {}))))))
    (fn [{:as qctx :keys [conn]} i+fcalls j+entities]
      (mfd/future
        (let [ent-id->js
              (->> j+entities
                (u/group-and-map-by
                  (fn [[j ent]] (id-attr ent))
                  (fn [[j ent]] j)))
              columns-infos->is
              (->> i+fcalls
                (u/group-and-map-by
                  (fn [[_i _fcall field-meta]]
                    (merge
                      (when-let [column-name (:myblog.sql/column-name field-meta)]
                        {:myblog.sql/required-columns [column-name]
                         :myblog.sql/read-value-from-row (fn [row] (get row column-name))})
                      (select-keys field-meta [:myblog.sql/post-process-fn
                                               :myblog.sql/discard-if-nil?
                                               :myblog.sql/required-columns
                                               :myblog.sql/read-value-from-row])))
                  (fn [[i _fcall _field-meta]]
                    i)))
              q [(str "SELECT "
                   (->> columns-infos->is keys
                     (mapcat :myblog.sql/required-columns)
                     (into #{primary-key-col})
                     (map name)
                     (str/join ", "))
                   " FROM " (name table-name)
                   " WHERE " (name primary-key-col) " = ANY (?)")
                 (into-array (keys ent-id->js))]]
          (d2q-res/into-resolver-result
            (for [row (jdbc/query conn q)
                  :let [ent-id (primary-key-col row)]
                  j (get ent-id->js ent-id)
                  [columns-info is] columns-infos->is
                  :let [{post-process-fn :myblog.sql/post-process-fn
                         discard-if-nil? :myblog.sql/discard-if-nil?
                         read-value-from-row :myblog.sql/read-value-from-row
                         :or {post-process-fn identity
                              discard-if-nil? false}} columns-info
                        raw-v (read-value-from-row row)
                        v (post-process-fn raw-v)]
                  :when (not (and discard-if-nil? (nil? v)))
                  i is]
              (d2q/result-cell j i v))))))))

;; ------------------------------------------------------------------------------
;; Server

(comment

  (def resolve-user-by-id
    (d2q-res/entities-independent-resolver
      (fn [{:as qctx :keys [conn]} i+fcalls]
        (mfd/future
          (letfn [(fcall-error [[i fcall]]
                    (let [user-id (-> fcall :d2q-fcall-arg :myblog.user/id)]
                      (when-not (string? user-id)
                        (ex-info
                          (str "Invalid or missing " (pr-str :myblog.user/id) " in " (pr-str :d2q-fcall-arg)
                            ", should be a String, got : " (pr-str user-id))
                          {:myblog.errors/error-type :myblog.error-types/invalid-fcall-arg
                           :d2q-fcall-i i}))))]
            (let [validation-errors (keep fcall-error i+fcalls)
                  good-fcalls (remove fcall-error i+fcalls)
                  user-id->is (u/group-and-map-by
                                (fn [[_i fcall]]
                                  (-> fcall :d2q-fcall-arg :myblog.user/id))
                                (fn [[i _fcall]] i)
                                good-fcalls)
                  q ["SELECT user_id FROM myblog_users WHERE user_id = ANY (?)"
                     (into-array String (keys user-id->is))]
                  found-user-ids
                  (into #{}
                    (map :user_id)
                    (jdbc/query conn q))
                  missing-user-ids (->> user-id->is keys (remove found-user-ids))
                  not-found-errors
                  (for [user-id missing-user-ids
                        i (get user-id->is user-id)]
                    (ex-info
                      (str "No " "User" " with " (pr-str :myblog.user/id) " : " (pr-str user-id))
                      {:myblog.errors/error-type :myblog.error-types/entity-not-found
                       :d2q-fcall-i i
                       :myblog.user/id user-id}))]
              {:d2q-errors (vec (concat validation-errors not-found-errors))
               :d2q-res-cells (vec
                                (for [user-id found-user-ids
                                      i (get user-id->is user-id)]
                                  (d2q/result-cell -1 i
                                    {:myblog.user/id user-id})))}))))))

  (defn resolve-user-rows
    [{:as qctx :keys [conn]} i+fcalls j+entities]
    (mfd/future
      (let [user-id->js
            (->> j+entities
              (filter (fn [[j ent]] (:myblog.user/id ent)))
              (u/group-and-map-by
                (fn [[j user]] (:myblog.user/id user))
                (fn [[j user]] j)))
            column-name->is
            (->> i+fcalls
              (u/group-and-map-by
                (fn [[i _fcall {:as field-meta, column-name :myblog.sql/column-name}]]
                  column-name)
                (fn [[i _fcall _field-meta]]
                  i)))
            q [(str "SELECT "
                 (->> column-name->is keys (into #{:user_id}) (map name) (str/join ", "))
                 " FROM " "myblog_users"
                 " WHERE " "user_id" " = ANY (?)")
               (into-array String (keys user-id->js))]]
        (d2q-res/into-resolver-result
          (for [row (jdbc/query conn q)
                :let [user-id (:user_id row)]
                j (get user-id->js user-id)
                [column-name is] column-name->is
                :let [v (get row column-name)]
                i is]
            (d2q/result-cell j i v))))))
  )

(def resolve-user-n-posts
  (d2q-res/fields-independent-resolver
    (fn [{:as qctx :keys [conn]} j+entities]
      (mfd/future
        (let [user-id->js
              (->> j+entities
                (filter (fn [[_j {user-id :myblog.user/id}]] user-id))
                (u/group-and-map-by
                  (fn [[_j {user-id :myblog.user/id}]] user-id)
                  (fn [[j _]] j)))
              q ["SELECT user_id, COUNT (post_id) AS n_posts FROM myblog_users LEFT JOIN myblog_posts ON myblog_users.user_id = myblog_posts.post_author_id WHERE user_id = ANY (?) GROUP BY user_id"
                 (into-array String (keys user-id->js))]]
          {:d2q-res-cells
           (vec
             (for [{:keys [user_id n_posts]} (jdbc/query conn q)
                   j (get user-id->js user_id)]
               (d2q/result-cell j -1 n_posts)))})))))


(def resolve-publication-last-n-comments
  (d2q-res/validating-fcall-args {}
    (fn [{:as qctx, :keys [conn]} i+fcalls j+entities]
      (mfd/future
        {:d2q-res-cells
         (vec (concat
                (let [post-id->js (->> j+entities
                                    (filter (fn [[_j ent]] (:myblog.post/id ent)))
                                    (u/group-and-map-by
                                      (fn [[_j ent]] (:myblog.post/id ent))
                                      first))
                      q-posts
                      ["SELECT
                     pub_post_id, ARRAY_AGG(comment_id) AS comments_ids
                     FROM (
                      SELECT
                        pub.post_id AS pub_post_id,
                        cmt.comment_id AS comment_id,
                        cmt.comment_published_at AS comment_published_at
                      FROM myblog_posts AS pub LEFT JOIN myblog_comments AS cmt ON pub.post_id = cmt.comment_about_post_id
                      WHERE pub.post_id = ANY (?)
                      ORDER BY comment_published_at DESC
                    ) AS x
                   GROUP BY pub_post_id"
                       (->> post-id->js keys (into-array String))]]
                  (for [{:keys [pub_post_id comments_ids]} (jdbc/query conn q-posts)
                        :let [all-comments
                              (->> comments_ids
                                (mapv (fn [comment-id]
                                        {:myblog.comment/id comment-id})))]
                        [i fcall] i+fcalls
                        :let [n (-> fcall :d2q-fcall-arg :n)
                              v (if (nil? n)
                                  all-comments
                                  (take n all-comments))]
                        j (get post-id->js pub_post_id)]
                    (d2q/result-cell j i v)))
                (let [comment-id->js (->> j+entities
                                       (filter (fn [[_j ent]] (:myblog.comment/id ent)))
                                       (u/group-and-map-by
                                         (fn [[_j ent]] (:myblog.comment/id ent))
                                         first))
                      q-comments
                      ["SELECT
                    pub_comment_id, ARRAY_AGG(comment_id) AS comments_ids
                    FROM (
                      SELECT
                        pub.comment_id AS pub_comment_id,
                        cmt.comment_id AS comment_id,
                        cmt.comment_published_at AS comment_published_at
                      FROM myblog_comments AS pub LEFT JOIN myblog_comments AS cmt ON pub.comment_id = cmt.comment_about_comment_id
                      WHERE pub.comment_id = ANY (?)
                      ORDER BY comment_published_at DESC
                    ) AS x
                    GROUP BY pub_comment_id"
                       (->> comment-id->js keys (into-array UUID))]]
                  (for [{:keys [pub_comment_id comments_ids]} (jdbc/query conn q-comments)
                        :let [all-comments
                              (->> comments_ids
                                (mapv (fn [comment-id]
                                        {:myblog.comment/id comment-id})))]
                        [i fcall] i+fcalls
                        :let [n (-> fcall :d2q-fcall-arg :n)
                              v (if (nil? n)
                                  all-comments
                                  (take n all-comments))]
                        j (get comment-id->js pub_comment_id)]
                    (d2q/result-cell j i v)))))}))))

(def fields
  schema/fields)

(defn checker-for-key
  [key valid-value?]
  (fn [d2q-fcall-arg]
    (let [ent-id (get d2q-fcall-arg key)]
      (if (valid-value? ent-id)
        true
        (throw (ex-info
                 (str "Invalid or missing " (pr-str key) " : " (pr-str ent-id))
                 {:myblog.errors/error-type :myblog.error-types/invalid-fcall-arg}))))))

(def resolvers
  [{:d2q.resolver/name :myblog.resolvers/user-by-id
    :d2q.resolver/compute
    (resolver-finding-entity-by-primary-key :myblog.user/id :myblog_users :user_id "User")
    :d2q.resolver/field->meta {:myblog/user-by-id {:d2q.helpers.field-meta/check-fcall-args
                                                   (checker-for-key :myblog.user/id string?)}}}
   {:d2q.resolver/name :myblog.resolvers/user-rows
    :d2q.resolver/compute
    (resolver-reading-table-rows :myblog_users_enriched :myblog.user/id :user_id)
    :d2q.resolver/field->meta {:myblog.user/id {:myblog.sql/column-name :user_id}
                               :myblog.user/first-name {:myblog.sql/column-name :user_first_name}
                               :myblog.user/last-name {:myblog.sql/column-name :user_last_name}
                               :myblog.user/full-name {:myblog.sql/column-name :user_full_name}}}
   {:d2q.resolver/name :myblog.resolvers/user-n-posts
    :d2q.resolver/compute #'resolve-user-n-posts
    :d2q.resolver/field->meta {:myblog.user/n-posts nil}}
   {:d2q.resolver/name :myblog.resolvers/post-by-id
    :d2q.resolver/compute
    (resolver-finding-entity-by-primary-key :myblog.post/id :myblog_posts :post_id "Blog Post")
    :d2q.resolver/field->meta {:myblog/post-by-id {:d2q.helpers.field-meta/check-fcall-args
                                                   (checker-for-key :myblog.post/id string?)}}}
   {:d2q.resolver/name :myblog.resolvers/post-rows
    :d2q.resolver/compute
    (resolver-reading-table-rows :myblog_posts :myblog.post/id :post_id)
    :d2q.resolver/field->meta {:myblog.post/id {:myblog.sql/column-name :post_id}
                               :myblog.post/title {:myblog.sql/column-name :post_title}
                               :myblog.post/content {:myblog.sql/column-name :post_content}
                               :myblog.post/published-at {:myblog.sql/column-name :post_published_at}
                               :myblog.post/author {:myblog.sql/column-name :post_author_id
                                                    :myblog.sql/post-process-fn
                                                    (fn [post_author_id]
                                                      (when-not (nil? post_author_id)
                                                        {:myblog.user/id post_author_id}))
                                                    :myblog.sql/discard-if-nil? true}}}
   {:d2q.resolver/name :myblog.resolvers/comment-by-id
    :d2q.resolver/compute
    (resolver-finding-entity-by-primary-key :myblog.comment/id :myblog_comments :comment_id "Blog Comment")
    :d2q.resolver/field->meta {:myblog/comment-by-id {:d2q.helpers.field-meta/check-fcall-args
                                                      (checker-for-key :myblog.comment/id #(instance? UUID %))}}}
   {:d2q.resolver/name :myblog.resolvers/comment-rows
    :d2q.resolver/compute
    (resolver-reading-table-rows :myblog_comments :myblog.comment/id :comment_id)
    :d2q.resolver/field->meta {:myblog.comment/id {:myblog.sql/column-name :comment_id}
                               :myblog.comment/content {:myblog.sql/column-name :comment_content}
                               :myblog.comment/published-at {:myblog.sql/column-name :comment_published_at}
                               :myblog.comment/author {:myblog.sql/column-name :comment_author_id
                                                       :myblog.sql/post-process-fn
                                                       (fn [comment_author_id]
                                                         (when-not (nil? comment_author_id)
                                                           {:myblog.user/id comment_author_id}))
                                                       :myblog.sql/discard-if-nil? true}
                               :myblog.comment/about {:myblog.sql/required-columns [:comment_about_post_id :comment_about_comment_id]
                                                      :myblog.sql/read-value-from-row
                                                      (fn [row]
                                                        (or
                                                          (when-some [post-id (:comment_about_post_id row)]
                                                            {:myblog.post/id post-id})
                                                          (when-some [comment-id (:comment_about_comment_id row)]
                                                            {:myblog.comment/id comment-id})
                                                          nil))
                                                      :myblog.sql/discard-if-nil? true}}}
   {:d2q.resolver/name :myblog.resolvers/publication-comments
    :d2q.resolver/compute #'resolve-publication-last-n-comments
    :d2q.resolver/field->meta {:myblog.publication/last-n-comments {:d2q.helpers.field-meta/check-fcall-args
                                                                    (fn [{:as fcall-arg :keys [n]}]
                                                                      (if (or
                                                                            (nil? n)
                                                                            (and (integer? n) (not (neg? n))))
                                                                        true
                                                                        (ex-info
                                                                          (str "Must have either nil or a nonegative integer at key " (pr-str :n))
                                                                          {:n n
                                                                           :myblog.errors/error-type :myblog.error-types/invalid-fcall-arg})))}}}])

(def server
  (d2q/server
    {:d2q.server/fields fields
     :d2q.server/resolvers resolvers}))

;; ------------------------------------------------------------------------------
;; Sandbox

(defn make-errors-nice
  ([d2q-res]
   (make-errors-nice d2q-res #{"clojure." "java.util.concurrent" "java.lang.Thread" "d2q.impl" "manifold."}))
  ([d2q-res prefix-blacklist]
   (supd/supdate
     d2q-res
     {:d2q-errors
      [[[Throwable->map
         {:trace
          #(into []
             (comp
               (map
                 (fn [^StackTraceElement c]
                   (if (or
                         (some
                           (fn [substr]
                             (-> c .getClassName (str/starts-with? substr)))
                           prefix-blacklist)
                         (= (.getMethodName c) "invokeStatic"))
                     '...
                     c)))
               (dedupe))
             %)}]]
       vec]}
     )))

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
       [{}])
    make-errors-nice)

  => '{:d2q-results [{"p1" {:myblog.post/title "Why French Bread is the best",
                            :myblog.post/author {:myblog.user/id "alice-hacker",
                                                 :myblog.user/first-name "Alice",
                                                 :myblog.user/last-name "P. Hacker",
                                                 :myblog.user/full-name "Alice P. Hacker",
                                                 :myblog.user/n-posts 1},
                            :last-5-comments [{:myblog.comment/id #uuid"ef72b664-c389-4be6-9a9e-0733833eddd5",
                                               :myblog.comment/content "Come on, American bread is not that bad!",
                                               :myblog.comment/published-at #inst"2018-10-10T08:12:28.243000000-00:00",
                                               :myblog.comment/about {:myblog.post/id "why-french-bread-is-the-best"}}]}}],
       :d2q-errors ({:cause "Invalid or missing :myblog.user/id : nil",
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
                            :message "Invalid :d2q-fcall-arg when calling Field :myblog/user-by-id. See the cause of this Exception for details.",
                            :at [clojure.core$ex_info invokeStatic "core.clj" 4617],
                            :data {:d2q-fcall-i 1}}
                           {:type clojure.lang.ExceptionInfo,
                            :message "Invalid or missing :myblog.user/id : nil",
                            :at [clojure.core$ex_info invokeStatic "core.clj" 4617],
                            :data {:myblog.errors/error-type :myblog.error-types/invalid-fcall-arg}}],
                     :trace [...
                             [d2q_examples.sql$checker_for_key$fn__62397 invoke "sql.clj" 427]
                             [d2q.helpers.resolvers$validating_fcall_args$fn__60829$fn__60831 invoke "resolvers.clj" 117]
                             ...
                             [d2q.helpers.resolvers$validating_fcall_args$fn__60829 invoke "resolvers.clj" 105]
                             ...
                             [manifold.deferred$eval56888$chain___56909 invoke "deferred.clj" 840]
                             ...
                             [d2q.api$query invoke "api.clj" 156]
                             ...
                             [d2q_examples.sql$eval62475 invoke "sql.clj" 539]
                             ...
                             [user$eval62471 invoke "form-init225402517162734634.clj" 1]
                             ...],
                     :data {:myblog.errors/error-type :myblog.error-types/invalid-fcall-arg}}
                     {:cause "No User with :myblog.user/id : \"USER-ID-THAT-DOES-NOT-EXIST\"",
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
                      :trace [...
                              [d2q_examples.sql$resolver_finding_entity_by_primary_key$fn__62083$f__56214__auto____62085$iter__62094__62100$fn__62101$iter__62096__62102$fn__62103$fn__62104
                               invoke
                               "sql.clj"
                               131]
                              [d2q_examples.sql$resolver_finding_entity_by_primary_key$fn__62083$f__56214__auto____62085$iter__62094__62100$fn__62101$iter__62096__62102$fn__62103
                               invoke
                               "sql.clj"
                               129]
                              ...
                              [d2q_examples.sql$resolver_finding_entity_by_primary_key$fn__62083$f__56214__auto____62085$iter__62094__62100$fn__62101
                               invoke
                               "sql.clj"
                               129]
                              ...
                              [d2q_examples.sql$resolver_finding_entity_by_primary_key$fn__62083$f__56214__auto____62085
                               invoke
                               "sql.clj"
                               136]
                              ...
                              [io.aleph.dirigiste.Executor$3 run "Executor.java" 318]
                              [io.aleph.dirigiste.Executor$Worker$1 run "Executor.java" 62]
                              [manifold.executor$thread_factory$reify__56096$f__56097 invoke "executor.clj" 44]
                              ...],
                      :data {:myblog.errors/error-type :myblog.error-types/entity-not-found,
                             :d2q-fcall-i 0,
                             :myblog.user/id "USER-ID-THAT-DOES-NOT-EXIST"}})}

  @(d2q/query server ctx
     [:myblog.user/n-posts]
     [{:myblog.user/id "alice-hacker"} {:myblog.user/id "john-doe"}])
  => {:d2q-results [{:myblog.user/n-posts 1}
                    {:myblog.user/n-posts 0}],
      :d2q-errors ()}

  (->
    @(d2q/query server ctx
       [:myblog.user/full-name]
       [{:myblog.user/id "alice-hacker"} {:myblog.user/id nil}])
    make-errors-nice)
  =>
  '{:d2q-results [{:myblog.user/full-name "Alice P. Hacker"} {}],
    :d2q-errors ({:cause "Invalid :myblog.user/id : nil",
                  :via [{:type clojure.lang.ExceptionInfo,
                         :message "Error in d2q Resolver :myblog.resolvers/user-rows",
                         :at [clojure.core$ex_info invokeStatic "core.clj" 4617],
                         :data {:d2q.error/type :d2q.error.type/resolver,
                                :d2q.resolver/name :myblog.resolvers/user-rows,
                                :d2q.error/query-trace ([:d2q.trace.op/resolver
                                                         {:d2q.resolver/name :myblog.resolvers/user-rows}]
                                                         [:d2q.trace.op/query
                                                          #d2q.datatypes.Query{:d2q-query-id nil,
                                                                               :d2q-query-fcalls [:...elided],
                                                                               :d2q-rev-query-path ()}])}}
                        {:type clojure.lang.ExceptionInfo,
                         :message "Invalid Entity passed to the Resolver. To fix, make sure your `check-entity` function is correct,  and that the upstream Resolver returns valid Entities. See the cause of this Exception for details.",
                         :at [clojure.core$ex_info invokeStatic "core.clj" 4617],
                         :data {:d2q-entcell-j 1}}
                        {:type clojure.lang.ExceptionInfo,
                         :message "Invalid :myblog.user/id : nil",
                         :at [clojure.core$ex_info invokeStatic "core.clj" 4617],
                         :data {}}],
                  :trace [...
                          [d2q_examples.sql$resolver_reading_table_rows$check_contains_id_attr__62152$fn__62154
                           invoke
                           "sql.clj"
                           154]
                          [d2q_examples.sql$resolver_reading_table_rows$check_contains_id_attr__62152 invoke "sql.clj" 153]
                          [d2q.helpers.resolvers$validating_input_entities$fn__60840$fn__60842 invoke "resolvers.clj" 161]
                          ...
                          [d2q.helpers.resolvers$validating_input_entities$fn__60840 invoke "resolvers.clj" 157]
                          ...
                          [manifold.deferred$eval56888$chain___56909 invoke "deferred.clj" 840]
                          ...
                          [d2q.api$query invoke "api.clj" 156]
                          ...
                          [d2q_examples.sql$eval62484 invoke "sql.clj" 729]
                          ...
                          [user$eval62480 invoke "form-init225402517162734634.clj" 1]
                          ...],
                  :data {}})}

  )
