(ns nl.sanderdijkhuis.log
  "Provides the DatabaseLogger component that writes out logs to
  SQLite database files. Log entries have keys that depend on the
  source channel, which is ideally constructed using log-chan.
  Entries are also printed to stdout.

  SQLite is used since it appears to be a reliable format. Multiple
  files are used (refreshed each hour) to reduce the impact of file
  corruption, i.e. not all data gets lost. Files are numbered
  subsequently and mostly independently from date/time, to reduce
  the risk of entries getting out of order when the system’s
  real-time clock is not working.

  Usage:

      (c/system-map {:logs {:foo (log/log-chan :foo)
                            :bar (log/log-chan :bar)}
                     :logger (c/using
                               (log/new-database-logger dir-path)
                               [:logs])})

  Also provides LogPrinter which can replace DatabaseLogger for
  debugging."

  (:require-macros
    [cljs.core.async.macros :refer [go go-loop]])
  (:require
    [cljs.core.async :as a :refer [<! >! chan close!]]
    [cljs.nodejs :as node]
    [clojure.string :as str]
    [goog.string :as gstr]
    [goog.string.format]
    [com.stuartsierra.component :as c])
  (:import
    [goog.i18n DateTimeFormat]))

(node/enable-util-print!)

;; Settings

(def chan-buf-size 100)
(def refresh-interval (* 60 60 1000))
(def time-format-js (DateTimeFormat. "yyyy-MM-dd HH:mm:ss.SSS"))
(def time-format-sqlite3 "%Y-%m-%d %H:%M:%f")

;; Log file handling

(defrecord Params [dirname series session part])

(defn path->params [path]
  (if-let [[[_ dirname series session-str part-str]]
           (re-seq #"^(.+)/log-series(\d+)-session(\d+)-part(\d+).db$" path)]
    (Params. dirname series (js/parseInt session-str 10) (js/parseInt part-str 10))))
(defn params->path [{:keys [dirname series session part]}]
  (gstr/format "%s/log-series%s-session%04d-part%04d.db" dirname series session part))

(defn new-series-name
  "Returns a string, e.g. 20151222."
  []
  (str/replace (.. (js/Date.) toISOString (substr 0 10)) #"-" ""))

(defn logs
  "Returns a sorted seq of Params records."
  [dirname]
  (->> (.. (node/require "fs") (readdirSync dirname))
       (map #(path->params (str dirname "/" %)))
       (filter some?)
       (sort-by :series)))

(defn new-params
  "Returns the Params for a new log file."
  [dirname]
  (if-let [p (last (logs dirname))]
    (-> p (update :session inc) (assoc :part 1))
    (Params. dirname (new-series-name) 1 1)))

(defn create-file
  "Promises a new SQLite3 db with the right table."
  [params]
  (let [sqlite3 (node/require "sqlite3")
        db (sqlite3.Database. (params->path params))
        result (a/promise-chan)]
    (. db run (str "create table if not exists log (
                      series, session, part, type, data,
                      time datetime default (strftime('" time-format-sqlite3 "', 'now')),
                      t_fact datetime
                    )")
      (fn [_]
        (a/put! result db)))
    result))

;; Log entry formatting

(defn log-line
  "Creates internal entry representations."
  [key]
  (fn [v] {:type key :data v :t-fact (.format time-format-js (js/Date.))}))

(defn xform [key]
  (map (log-line key)))

(defn log-chan
  "Creates a channel with keywords and fact times added."
  ([key xform] (chan chan-buf-size (comp xform (map (log-line key)))))
  ([key] (chan chan-buf-size (map (log-line key)))))

;; Database logger

(defrecord State [params db timeout])

(defn new-state [this]
  (let [ps (new-params (:dirname this))]
    (go (State. ps
                (<! (create-file ps))
                (a/timeout refresh-interval)))))

(defn add-log [this {:keys [db params] :as state} {:keys [type data t-fact] :as entry}]
  (let [result (a/promise-chan)
        {:keys [series session part]} params]
    (println type data)
    (. db run
      "insert into log (series, session, part, type, data, t_fact) values (?, ?, ?, ?, ?, ?)"
      series session part (name type) (pr-str data) t-fact
      #(if %
         (println "LOG ERROR: " %)
         (a/put! result state)))
    result))

(defn refresh-db [this {:keys [db] :as state}]
  (let [params' (-> (:params state) (update :part inc))]
    (.close db)
    (go (State. params'
                (<! (create-file params'))
                (a/timeout refresh-interval)))))

(defn close [this {:keys [db] :as state}]
  (let [p (a/promise-chan)]
    (.close db #(a/put! p true))
    p))

(defn log-loop [{:keys [combined] :as this}]
  (go-loop [{:keys [params db timeout] :as state} (<! (new-state this))]
    (let [[v port] (a/alts! [combined timeout])]
      (cond (= port timeout) (recur (<! (refresh-db this state)))
            v                (recur (<! (add-log this state v)))
            :else            (<! (close this state)))))
  this)

(defrecord DatabaseLogger [dirname logs combined]
  c/Lifecycle

  (start [this]
    (log-loop (assoc this :combined (a/merge (vals logs)))))

  (stop [this]
    (close! combined)
    (dissoc this :combined)))

(defn new-database-logger [dirname]
  (map->DatabaseLogger {:dirname dirname}))

;; Log printer (stdout)

(defrecord LogPrinter [combined logs]
  c/Lifecycle

  (start [this]
    (let [combined (a/merge (vals logs))]
      (go-loop [{:keys [type data session] :as line} (<! combined)]
        (when line
          (println (str "[LOG " session type "]") data)
          (recur (<! combined))))
      (assoc this :combined combined)))

  (stop [this]
    (close! combined)
    (dissoc this :combined)))

(defn new-log-printer []
  (map->LogPrinter {}))
