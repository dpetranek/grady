(ns grady
  (:require [clojure.data.csv :as csv]
            [clojure.tools.cli :as cli]
            [promesa.core :as p]
            [promesa.exec.csp :as sp]
            [malli.core :as m]
            [org.httpkit.client :as http]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (org.jsoup Jsoup))
  (:gen-class))

(defn write
  [path content]
  (let [file  (io/file path)
        bytes (.getBytes content)]
    (try
      (with-open [out (io/output-stream file)]
        (.write out bytes))
      (catch Exception e
        (println "Unable to write output file " path "."
                 (.getMessage e))))))

(defn select-text
  [selectors body]
  (let [doc     (Jsoup/parse body)
        matches (->> (.select doc selectors)
                     (map #(.text %)))]
    (reduce #(str %1 "\n" %2) "" matches)))

(defn fetch-page!
  [{:keys [url] :as spec}]
  (println "Fetching" url)
  (assoc spec :resp @(http/request {:url url :method :get})))

(defn check-response
  [{:keys [resp] :as spec}]
  (println (:url spec) "response" (:status resp))
  (if (#{200 201} (:status resp))
    (assoc spec :body (:body resp) )
    (assoc spec :error-resp resp)))

(defn process-body
  [{:keys [body selectors] :as spec}]
  (println (:url spec) "processing" selectors)
  (if body
    (assoc spec :text (select-text selectors body))
    spec))

(defn write-text!
  [out {:keys [text id] :as spec}]
  (println (:url spec) "writing" id)
  (if text
    (let [filename (str id ".txt")
          path (str out "/" filename)]
      (write path text)
      spec)
    spec))

(defn build-report
  [{:keys [resp] :as spec}]
  (-> spec
      (assoc :status (:status resp))
      (select-keys [:status :id :url :selectors])))

(defn write-report!
  [out results]
  (let [results (map build-report results)
        cols    [:id :url :selectors :status]
        rows    (into [(mapv name cols)] (map (apply juxt cols) results))
        path    (str out "output-summary_" (System/currentTimeMillis) ".csv")]
    (with-open [writer (io/writer (io/file path))]
      (csv/write-csv writer rows ))))

(defn ch-into
  [ch]
  (loop [res []]
    (if-let [x (sp/take! ch)]
      (recur (conj res x))
      res)))

(defn work
  [out input]
  (let [out-ch (sp/chan 1 (comp
                            (map check-response)
                            (map process-body)
                            (map (partial write-text! out))))
        in-ch (sp/chan)]
    (sp/onto-chan! in-ch input)
    (sp/pipeline :typ :vthread
                 :n 2
                 :in in-ch
                 :out out-ch
                 :f (fn [spec ch]
                      (-> (sp/go-chan (fetch-page! spec))
                          (sp/pipe ch))))
    (ch-into out-ch)))

(defn parse-csv
  [in]
  (let [[cols & rows] (csv/read-csv in)]
    (map (partial zipmap (map keyword cols)) rows)))

(defn read-csv
  [in]
  (parse-csv (slurp in)))

(def ScrapeSpec
  [:map
   [:id :string]
   [:url :string]
   [:selectors :string]])

(defn valid-csv?
  [csv-data]
  (m/validate [:sequential ScrapeSpec] csv-data))

(defn ensure-trailing-slash
  [s]
  (cond (nil? s)
        "./"
        (str/ends-with? s "/")
        s
        :else
        (str s "/")))

(defn ensure-out-dir
  [out]
  (io/make-parents (io/file (str out "x")))
  true)

(def cli-options
  [[nil "--in PATH_TO_INPUT_CSV" "Path to input csv"
    :parse-fn read-csv
    :validate [valid-csv? "Must be a path to a csv file with columns: id, url, selectors"]]
   [nil "--out PATH_TO_OUTPUT_DIR" "Path to output dir"
    :parse-fn ensure-trailing-slash
    :validate [ensure-out-dir "Invalid out directory."]]
   ["-h" "--help"]])

(def usage
  "This is a program for fetching the text from a list of urls stored in a csv
  file, saving each url's text in a txt file. The csv file should have at least three
  columns: \"id\", \"url\", \"selectors\".

id - should contain an identifier that will be used as the filename for the output txt file
url - the url of the website to scrape text from
selectors - the css selectors used to identify which parts of the website should be targeted

Usage: java -jar grady.jar --in=<path-to-input-csv> --out=<path-to-output-dir>
")

(defn exit
  [status msg]
  (println msg)
  (System/exit status))


(defn error-msg
  [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

(defn validate-args
  [args]
  (let [{:keys [options errors]} (cli/parse-opts args cli-options)]
    (cond (:help options)
          {:exit-msg usage :ok? true}

          errors
          {:exit-msg (error-msg errors) :ok? false}

          (not (:in options))
          {:exit-msg "Must specify input csv file with --in" :ok? false}

          :else
          {:options options})))

(defn -main
  [& args]
  (let [{:keys [options exit-msg ok?]} (cli/parse-opts args cli-options)]
    (if exit-msg
      (exit (if ok? 0 1) exit-msg)
      (let [{:keys [out in]} options
            results (work out in)]
        (write-report! out results)
        (exit (if (zero? (count (filter :error-resp results))) 0 1)
              (str "Successfully processed " (count (remove :error-resp results)) "/" (count in)))))))
