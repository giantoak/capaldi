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

Think of the data as a set of potential of different time-based buckets.
Assuming that a particular slice bucketing doesn't violate a given algorithm's
assumptions, we can run the bucket through the algorithm and then assess the
results. Information about the success of one algorithm may help us
automatically eliminate other algorithms from the running.

We can thus think of the space as a huge graph of candidate data slices,
algorithms, and statistical validity checks.

To this end, Capaldi is implemented as a Python module that uses
[`luigi`](https://luigi.readthedocs.io) to slice data and run validity checks
and different algorithms in batch. Algorithms can block until their various
dependencies are met or they have been eliminated from the pool of analyses
that should be run.

The module is defined in the `capaldi` directory. With the repository, the
`algs` directory includes submodules for each individual time series
algorithm. Many of these submodules are wrappers for R packages that must
be hosted on a partner [OpenCPU](https://www.opencpu.org/) instance; OpenCPU
can be run either locally or remotely.

Sanity checks are currently hosted in `algs` as well but will be getting moved
to a new `checks` module. Many of the algorithms depend on the same assumptions
so these can be collapsed to a simple checklist.

`capaldi.py` is primarily a wrapper for command line input that sets up a local,
temporary working directory. It invokes `CapaldiWrapper.py`, a
[`luigi.WrapperTask`](https://github.com/spotify/luigi/blob/master/luigi/task.py#L631)
that sanity checks the data, ingests it into an [HDF5 file](http://www.h5py.org/), and
triggers all of the requested time series algorithms.

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
Kalman Filters |[source](https://pykalman.github.io/)

#### Planned

Algorithm |Source
:--- |:---
... |...
