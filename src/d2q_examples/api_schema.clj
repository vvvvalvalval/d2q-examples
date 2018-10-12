(ns d2q-examples.api-schema)

(def fields
  [{:d2q.field/name :myblog/user-by-id
    :d2q.field/ref? true
    :d2q.field/cardinality :d2q.field.cardinality/one}

   {:d2q.field/name :myblog.user/id
    :d2q.field/ref? false}
   {:d2q.field/name :myblog.user/first-name
    :d2q.field/ref? false}
   {:d2q.field/name :myblog.user/last-name
    :d2q.field/ref? false}
   {:d2q.field/name :myblog.user/full-name
    :d2q.field/ref? false}
   {:d2q.field/name :myblog.user/n-posts
    :d2q.field/ref? false}

   {:d2q.field/name :myblog/post-by-id
    :d2q.field/ref? true
    :d2q.field/cardinality :d2q.field.cardinality/one}

   {:d2q.field/name :myblog.post/id
    :d2q.field/ref? false}
   {:d2q.field/name :myblog.post/title
    :d2q.field/ref? false}
   {:d2q.field/name :myblog.post/content
    :d2q.field/ref? false}
   {:d2q.field/name :myblog.post/published-at
    :d2q.field/ref? false}
   {:d2q.field/name :myblog.post/author
    :d2q.field/ref? true
    :d2q.field/cardinality :d2q.field.cardinality/one}

   {:d2q.field/name :myblog/comment-by-id
    :d2q.field/ref? true
    :d2q.field/cardinality :d2q.field.cardinality/one}

   {:d2q.field/name :myblog.comment/id
    :d2q.field/ref? false}
   {:d2q.field/name :myblog.comment/content
    :d2q.field/ref? false}
   {:d2q.field/name :myblog.comment/published-at
    :d2q.field/ref? false}
   {:d2q.field/name :myblog.comment/author
    :d2q.field/ref? true
    :d2q.field/cardinality :d2q.field.cardinality/one}
   {:d2q.field/name :myblog.comment/about
    :d2q.field/ref? true
    :d2q.field/cardinality :d2q.field.cardinality/one}

   {:d2q.field/name :myblog.publication/last-n-comments
    :d2q.field/ref? true
    :d2q.field/cardinality :d2q.field.cardinality/many}
   ])
