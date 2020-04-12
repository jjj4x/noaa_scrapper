# NOAA (NCDC) Scrapper

Parallel scrapper for [U.S. Local Climatological Data Archive](https://www.ncei.noaa.gov/data/global-hourly/archive/isd/).


# Outline

* [License](#license)
* [Notes and Overview](#notes-and-overview)
* [Usage](#usage)
  * [Install](#install)
  * [Uninstall](#uninstall)
  * [Help](#help)
  * [Examples](#examples)


# License
[-> Outline](#outline)

This project is licensed under the Apache 2.0 Licence. See [LICENCE](./LICENSE)
for more information.


# Notes and Overview
[-> Outline](#outline)

Parallel scrapper for [U.S. Local Climatological Data Archive](https://www.ncei.noaa.gov/data/global-hourly/archive/isd/).
The loading and decompression happens in parallel. The data is aggregated into
yearly files with optional GNU Zip compression.

The scrapper is written for downloading samples for
[Hadoop: The Definitive Guide](https://github.com/tomwhite/hadoop-book/) book.
I've found the book's instructions unclear; also, the sequential loading of
samples takes too much time, so, the scrapper leverages
[multiprocessing](https://docs.python.org/3/library/multiprocessing.html)
to speed things up.


# Usage
[-> Outline](#outline)

## Install
[-> Outline](#outline)

```shell script
git clone https://github.com/jjj4x/noaa_scrapper.git
cd noaa_scrapper
pip install .  # or "python setup.py install"
```

Or, install from the GitHub transparently:
```shell script
pip install git+git://github.com/jjj4x/noaa_scrapper.git
```

## Uninstall
[-> Outline](#outline)

```shell script
pip uninstall noaa_scrapper
```

## Help
[-> Outline](#outline)

Available options:
```shell script
noaa-scrapper --help
```

```text
usage: noaa-scrapper [-h] [--url URL] [--index-regex INDEX_REGEX]
                     [--member-regex MEMBER_REGEX]
                     [--run-time-max RUN_TIME_MAX]
                     [--workers-count WORKERS_COUNT]
                     [--polling-timeout POLLING_TIMEOUT]
                     [--terminate_timeout TERMINATE_TIMEOUT] [--years YEARS]
                     [--force] [--tmp-dir TMP_DIR] [--is-compress]

optional arguments:
  -h, --help            show this help message and exit
  --url URL
  --index-regex INDEX_REGEX
  --member-regex MEMBER_REGEX
  --run-time-max RUN_TIME_MAX
  --workers-count WORKERS_COUNT
  --polling-timeout POLLING_TIMEOUT
  --terminate_timeout TERMINATE_TIMEOUT
  --years YEARS         For example: --years 1901; --years 1901,1902; --years
                        1901-1930
  --force               Force overwrite files if they already exist.
  --tmp-dir TMP_DIR     Directory for dumping temporary data (tarball
                        extraction).
  --is-compress         If set, the result will be gzipped in filename like
                        "1901.gz". Else it will be saved as plaintext into
                        "1901".
```

Samples will be loaded into **PWD**.

It makes sense to **use more workers than the cores available**,
because the downloading part is IO-bound.

It **doesn't make sense to use more workers than years** (e.g., 10 workers for 4 yers files).

The compression and decompression parts are CPU-bound.

If compression isn't used (by default), the results will be plaintext files.

## Examples
[-> Outline](#outline)

The defaults are 2 workers, 1901 and 1902 years, without compression:
```shell script
noaa-scrapper

# Will list 1901  1902
ls 190[1-2]
```

The files won't be loaded for a second time without --force flag:
```shell script
noaa-scrapper --force
```

To download some range:
```shell script
noaa-scrapper --workers-count 12 --years 1901-1912

# Will list 1901  1902  1903  1904  1905  1906  1907  1908  1909  1910  1911  1912
ls 19??
```

To download some range and compress the results:
```shell script
noaa-scrapper --workers-count 24 --years 1901-1924 --is-compress

# The 24 files are downloaded
ls *.gz | tr '\s' '\n' | wc -l
```

Profile it with **time** to find-out the --workers-count for your hardware:
```shell script
time noaa-scrapper --workers-count 30 --years 1925-1955 --is-compress
```

If your runtime is too long, the master might be shutdown before the work is done.
Use **--run-time-max** to specify the number of seconds (300 by default):
```shell script
noaa-scrapper --workers-count 30 --years 1925-1955 --is-compress --run-time-max 600
```

Disable noisy output by shell redirection (the logging set up into STDERR):
```shell script
noaa-scrapper --workers-count 30 --years 1925-1955 --is-compress --run-time-max 600 2> /dev/null
```
