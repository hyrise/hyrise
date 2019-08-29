The data was generated as follows (see dbgen_version.csv):
$ ./dsdgen -scale 1 -terminate N -rngseed 305 -suffix .csv
dsdgen does not support scale factors < 1, but we don't want to check the ~1.2GB of data.
So we only want the first 50 rows per table:
$ for f in *.csv; do mv $f $f.big; head -n 50 $f.big > $f; rm $f.big; done