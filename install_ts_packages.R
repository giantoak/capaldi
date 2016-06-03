install.packages(c(
"bcp",
"forecast",
"devtools"))

library("devtools")

devtools::install_github(c(
"giantoak/goarima",
"giantoak/mmppr",
"google/CausalImpact",
"twitter/AnomalyDetection",
"twitter/BreakoutDetection"))