This is a program that ingests a csv of urls and returns the text for those urls


## build

install these prerequisites:

[JDK 17+](https://adoptium.net)

[clojure](https://clojure.org/guides/install_clojure)

And then run
```
clj -T:build uber
```

This will produce a jar file in the `target` directory: `target/grady-0.0.1-standalone.jar`.

## run

To run the program you will need to execute this command in a terminal:

```
# ex:
java -jar target/grady-0.0.1-standalone.jar --in=<path-to-input-csv-file> --out=<path-to-output-dir>
```

For each url in the `url` column of the csv, the site will be fetched and scraped for
text according to the css selectors specified in the `selectors` column of the csv. That
text for each site will then be written to the output dir in a file named `<id>.txt`,
where `<id>` corresponds to the text in the `id` column of the input csv.

## notes

Some sites may have measures in place that prevent naive scraping like this. In order to
tell which sites were successfully scraped you can review the output summary csv in the
output directory. The `status` column will show the HTTP status code of the request -
200 or 201 indicates a success, something 300 or greater, or a missing status, indicates
an error, and no output will be created for that url.
