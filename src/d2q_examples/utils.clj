(ns d2q-examples.utils)

(defn group-and-map-by
  [kf vf coll]
  (reduce
    (fn [m x]
      (let [k (kf x)
            v (vf x)]
        (update m k (fn [xs] (-> xs (or []) (conj v))))))
    {} coll))
