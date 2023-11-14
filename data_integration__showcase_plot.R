library(dplyr)
library(ggplot2)
library(ggthemes)
library(stringr)
library(tidyr)

if (Sys.getenv("RSTUDIO") == "1") {
  setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
}
print(getwd())
source("ggplot_theme.R")


results <- read.csv("data_loading_main.csv")
results$RUNTIME_S <- results$RUNTIME_NS / 1000 / 1000 / 1000
results$SCHEDULER_MODE <- as.factor(results$SCHEDULER_MODE)
results$BENCHMARK <- as.factor(results$BENCHMARK)
results$SCALE_FACTOR <- as.factor(results$SCALE_FACTOR)
results$RADIX_CLUSTER_FACTOR <- as.factor(results$RADIX_CLUSTER_FACTOR)

levels(results$SCALE_FACTOR) <- list("Scale Factor 10" = 10,
                                     "Scale Factor 1" = 1)

results <- extract(results, NAME, into = c('discard', 'BENCHMARK_ITEM'), '(.*)\\s+([^ ]+)$')

results_agg <- results %>% group_by(BENCHMARK, SCALE_FACTOR, SCHEDULER_MODE, BENCHMARK_ITEM, RADIX_CLUSTER_FACTOR) %>%
                           summarize(AVG_SINGLE_RUNTIME_S = mean(RUNTIME_S), .groups="keep")

results_agg_agg <- results_agg %>% group_by(BENCHMARK, SCALE_FACTOR, SCHEDULER_MODE, RADIX_CLUSTER_FACTOR) %>%
                                   summarize(AVG_RUNTIME_S = mean(AVG_SINGLE_RUNTIME_S), .groups="keep")

results_agg_agg_norm <- results_agg_agg %>% group_by(BENCHMARK, SCALE_FACTOR, SCHEDULER_MODE) %>%
                                            mutate(NORM_AVG_RUNTIME_S = AVG_RUNTIME_S / min(AVG_RUNTIME_S), .groups="keep")

ggplot(results_agg_agg_norm %>% filter(SCHEDULER_MODE == "mt"),
       aes(x=RADIX_CLUSTER_FACTOR, group=BENCHMARK, y=NORM_AVG_RUNTIME_S, fill=BENCHMARK, shape=BENCHMARK, color=BENCHMARK)) +
  geom_line() +
  geom_point() +
  theme_bw() +
  scale_colour_tableau(palette="Superfishel Stone") +
  scale_fill_tableau(palette="Superfishel Stone") +
  theme.paper_plot +
  facet_wrap( ~ SCALE_FACTOR, ncol=2, scales = "free_y") +
  coord_cartesian(clip = "off") +
  labs(x= "Radix Cluster Factor", y="Cumu. Avg. Runtime [s]") +
  theme(legend.position="top")

plot <- ggplot(results_agg_agg_norm %>% filter(SCHEDULER_MODE == "mt"),
               aes(x=RADIX_CLUSTER_FACTOR, group=BENCHMARK, y=NORM_AVG_RUNTIME_S, fill=BENCHMARK, shape=BENCHMARK, color=BENCHMARK)) +
  geom_line() +
  geom_point() +
  theme_bw() +
  scale_colour_tableau(palette="Superfishel Stone") +
  scale_fill_tableau(palette="Superfishel Stone") +
  theme.paper_plot +
  facet_wrap( ~ SCALE_FACTOR, ncol=2, scales = "free_y") +
  labs(x= "Radix Cluster Factor", y="Norm. Cumu. Runtime") +
  theme(legend.position=c(.75,.675)) +
  theme(legend.title = element_blank()) +
  theme(legend.background=element_blank()) +
  theme(legend.key.size = unit(4, "mm")) +
  theme(plot.margin=unit(c(1,1,0,1), 'mm')) +
  theme(axis.title.y = element_text(hjust=0.7))
print(plot)
ggsave("radix_cluster_plot.pdf", plot, width=5, height=1.25)
