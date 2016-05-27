Capyldi is a Python-based time series expert system; currently, it takes data, transforms it, and bulk executes a number of time-series algorithms while providing warnings and alarms. Some interpretive information is included at present, but more will be bundled in in the future. While it won't substitute for a PhD, hopefully it will eventually substitute for a very limited version of one. 

The currently incorporated algorithms:

* Markov-Modulated Poisson Point Processes ([MMPP](https://github.com/giantoak/mmppr))
* ARIMA
* E-Divisive With Medians ([Twitter's Breakout Detection](https://github.com/twitter/BreakoutDetection))

Several additional algorithms are almost incorporated, but need some tweaking.

Capyldi uses Python locally for data transformations and what stats, then hits up a user-specified install of [OpenCPU](https://www.opencpu.org/) as needed for R calculations. 
