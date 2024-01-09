(ns net.drilling.plugins.switchboard
  (:require [clojure.core.async :as a]
            [medley.core]))

(def namespace-to-subdir-registry
  {"<bus" :buses
   "<consumer" :consumers
   "<mult" :mults
   "<pub" :pubs
   "<tap" :taps})

(defn subdir-for [k]
  (get namespace-to-subdir-registry (namespace k)))

(defn dir-path-for
  ([id]
   [:dir (subdir-for id) id])
  ([ln id]
   [:dir (subdir-for ln) ln (subdir-for id) id]))

(defn dir-add
  ([{:keys [switchboard id v ln]}]
   (if-some [_ ln]
     (dir-add switchboard ln id v)
     (dir-add switchboard id v)))
  ([switchboard id v]
   (assoc-in switchboard (dir-path-for id) v))
  ([switchboard ln id v]
   (assoc-in switchboard (dir-path-for ln id) v)))

(defn add-chan!
  "Generic add channel function. If a channel is provided, it will be used. Otherwise, a new channel will be created."
  ([{:keys [switchboard id ch]}]
   (if-some [_ ch]
     (add-chan! switchboard id ch)
     (add-chan! switchboard id)))
  ([switchboard ch-id] (add-chan! switchboard ch-id (a/chan)))
  ([switchboard ch-id ch]
   (as-> switchboard $
     (assoc $ ch-id ch)
     (assoc-in $ [:dir :chans ch-id] ch)
     (if (= :<bus (namespace ch-id))
       (dir-add $ ch-id {:chan ch})
       $))))

(defn add-mult!
  "Creates a new mult, links it to it's source and adds it to the bulletin board."
  ([{:keys [switchboard id ln]}] (add-mult! switchboard id ln))
  ([switchboard mult-id src-id]
   (let [src-ch (get switchboard src-id)
         mult-ch (a/mult src-ch)]
     (as-> switchboard $
       (add-chan! $ mult-id mult-ch)
       (dir-add $ mult-id {:chan mult-ch
                           :src src-id
                           :src-ch src-ch})
       (dir-add $ src-id mult-id mult-ch)))))

(defn add-tap!
  "Creates a tap, taps it into a mult and adds it to the bulletin board."
  ([{:keys [switchboard id ln val]}]
   (if-some [_ val]
     (add-tap! switchboard id val ln)
     (add-tap! switchboard id ln)))
  ([switchboard tap-ident mult-ident] (add-tap! switchboard  tap-ident (a/chan) mult-ident))
  ([switchboard tap-ident tap-ch mult-ident]
   (let [mult-ch (get switchboard mult-ident)
         _ (a/tap mult-ch tap-ch)]
     (as-> switchboard $
       (add-chan! $ tap-ident tap-ch)
       (dir-add $ tap-ident {:chan tap-ch
                             :mult mult-ident
                             :mult-chan mult-ch})
       (dir-add $ mult-ident tap-ident tap-ch)))))

(defn add-pub!
  "Adds a pub, linking it to a source, and adds it to the bulletin board."
  ([{:keys [switchboard id ln rule]}]
   (add-pub! switchboard id ln rule))
  ([switchboard pub-ident src-ident rule]
   (let [src-ch (get switchboard src-ident)
         pub-ch (a/pub src-ch rule)]
     (as-> switchboard $
       (add-chan! $ pub-ident pub-ch)
       (dir-add $ pub-ident {:chan pub-ch
                             :source src-ident
                             :source-chan src-ch
                             :rule rule})
       (dir-add $ src-ident pub-ident pub-ch)))))

(defn default-consumer [ch f]
  (a/go-loop []
    (when-let [evt (a/<! ch)]
      (apply f [evt])
      (recur))))

(defn call-fn-from-string
  [fqs]
  #?(:clj (if (fn? fqs)
            fqs
            (let [split-var (clojure.string/split fqs #"\/")
                  nmsp (first split-var)
                  n (str (second split-var))
                  _ (require (symbol nmsp))]
              (resolve (symbol nmsp n))))
     :cljs (clojure.edn/read-string fqs)))

(defn add-consumer!
  ([{:keys [switchboard id rule topic consume-with]}] (add-consumer! switchboard id rule topic consume-with))
  ([switchboard id rule topic consume-with]
   (let [pub-ident (case rule
                     :event/ident :<pub/event-ident
                     :event/source :<pub/event-source
                     :event/target :<pub/event-target)
         pub-ch (get switchboard pub-ident)
         sub-ch (a/chan)
         _ (a/sub pub-ch topic sub-ch)
         f (call-fn-from-string consume-with)
         _ (default-consumer sub-ch f)]
     (as-> switchboard $
       (add-chan! $ id sub-ch)
       (dir-add $ id {:chan sub-ch
                      :topic topic
                      :rule rule
                      :consume-with consume-with})))))

(def ns->creation-fn-registry
  {"<chan" add-chan!
   "<bus" add-chan!
   "<mult" add-mult!
   "<tap" add-tap!
   "<pub" add-pub!
   "<consumer" add-consumer!})

(defn switchboard!
  "Wires a channel into the bulletin board."
  [& ms]
  (let [{:keys [id] :as m} (apply merge ms)
        f (get ns->creation-fn-registry (namespace id))]
    (f m)))

(defn add-to-switchboard!
  [switchboard comms]
  (loop [chs (seq (vec (flatten comms)))
         results [switchboard]]
    (if chs
      (let [ch (first chs)
            working-switchboard (last results)
            new-result (switchboard! {:switchboard working-switchboard} ch)]
        (recur (next chs) (conj results new-result)))
      (last results))))

(defn bootstrap-switchboard! [event-bus]
  (add-to-switchboard! {} [[{:id :<bus/eb :ch event-bus}]
                     [{:id :<mult/eb :ln :<bus/eb}]
                     [{:id :<tap/event-ident :ln :<mult/eb}
                      {:id :<tap/event-source :ln :<mult/eb}
                      {:id :<tap/event-target :ln :<mult/eb}]
                     [{:id :<pub/event-ident :ln :<tap/event-ident :rule :event/ident}
                      {:id :<pub/event-source :ln :<tap/event-source :rule :event/source}
                      {:id :<pub/event-target :ln :<tap/event-target :rule :event/target}]]))
