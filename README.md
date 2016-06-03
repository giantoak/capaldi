## The Idea

Capaldi is a Python-and-R-based time series expert system. It's designed to
take a generic time series - that is, floating point or integer values at a
sequence of times, and return:

* A list of candidate models fit to the data.
* Some basic information about why these fitted models are suitable
* Some basic information about why other candidate models can't be
reasonably fit.

This information will be returned as a mix of text and graphs, packed into a
JSON Object.


## Status

Currently, Capaldi is set up to take a generic time series, regroup the data
into a number of different date/time buckets, and run a few algorithms in bulk
on it. It provides some minimal sanity checking based on the number of buckets
produced and the some of the properties of the data.

It is *not* yet set up to provide the planned expert feedback, plain text
responses, or pretty graphics.

### Design

Capaldi is implemented as a Python module, defined in the `capaldi` directory.
With the repository, the `algs` directory includes submodules for each
individual time series algorithm. Many of these submodules are wrappers
for R packages that must be hosted on a partner
[OpenCPU](https://www.opencpu.org/) instance; OpenCPU will serve a local
installation of R and make it accessible via a JSON API.

Sanity checks are currently hosted in `algs` as well but will be getting moved
to a new `checks` module. Many of the algorithms depend on the same assumptions
so these can be collapsed to simple checklist.

The principal function -`capaldi()` in `capaldi/capaldi.py` takes a data frame,
divides it into a number of chunks, tests each independence assumption, and then runs each algorithm on each chunk.



### Algorithms
#### Currently-incorporated

Algorithm |Source
:--- |:---
ARIMA |[source](https://github.com/giantoak/goarima)
Markov-Modulated Poisson Processes (MMPP) |[source](https://github.com/giantoak/mmppr)
Barry and Hartigan's Product Partition Model |[source](https://cran.r-project.org/web/packages/bcp/index.html)

#### In-progress

Algorithm |Source
:--- |:---
Bayesian Structural Time Series / Google Causal Impact |[source](https://google.github.io/CausalImpact/CausalImpact.html)
Seasonal Hybrid ESD (S-H-ESD) / Twitter Anomaly Detection |[source](https://github.com/twitter/AnomalyDetection)
E-Divisive With Medians (EDM) / Twitter Breakout Detection |[source](https://github.com/twitter/BreakoutDetection)

#### Planned

Algorithm |Source
:--- |:---
Kalman Filters |[source](https://pykalman.github.io/)
... |...
