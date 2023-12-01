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


results <- read.csv("data_loading_main_argument.csv")
results$RUNTIME_S <- results$RUNTIME_NS / 1000 / 1000 / 1000
results$SCHEDULER_MODE <- as.factor(results$SCHEDULER_MODE)
results$BENCHMARK <- as.factor(results$BENCHMARK)
results$SCALE_FACTOR <- as.factor(results$SCALE_FACTOR)

results <- extract(results, NAME, into = c('discard', 'BENCHMARK_ITEM'), '(.*)\\s+([^ ]+)$')

results_agg <- results %>% group_by(BENCHMARK, SCALE_FACTOR, SCHEDULER_MODE, BENCHMARK_ITEM) %>%
                           summarize(AVG_RUNTIME_S = mean(RUNTIME_S), .groups="keep")

results_query_grouped <- results_agg %>% filter(BENCHMARK != "JCC-H (normal)") %>%
                                         group_by(SCALE_FACTOR, SCHEDULER_MODE, BENCHMARK_ITEM) %>%
                                         mutate(NORM_RUNTIME = AVG_RUNTIME_S / min(AVG_RUNTIME_S), .groups="keep")

ggplot(results_agg %>% filter(SCHEDULER_MODE == "mt"),
       aes(x=BENCHMARK_ITEM, group=BENCHMARK, y=AVG_RUNTIME_S, fill=BENCHMARK)) +
  geom_col(position = "dodge") +
  # geom_hline(data = query_runtimes, aes(yintercept = MEAN_RUNTIME)) +
  theme_bw() +
  scale_colour_tableau(palette="Superfishel Stone") +
  scale_fill_tableau(palette="Superfishel Stone", name="Step:", guide = guide_legend(reverse=TRUE)) +
  theme.paper_plot +
  facet_wrap( ~ SCALE_FACTOR, ncol=1, scales = "free_y") +
  coord_cartesian(clip = "off") +
  labs(x= "Threading Configuration", y="Runtime [s]") +
  theme(legend.position="top")


query_selection_argument <- ggplot(results_query_grouped %>% filter(SCHEDULER_MODE == "mt") %>% filter(SCALE_FACTOR == 10),
                                   aes(x=BENCHMARK_ITEM, group=BENCHMARK, y=NORM_RUNTIME, fill=BENCHMARK)) +
  geom_hline(aes(yintercept = 1.0), color="#adadad") +
  geom_col(position = "dodge") +
  theme_bw() +
  scale_colour_tableau(palette="Superfishel Stone") +
  scale_fill_tableau(palette="Superfishel Stone") +
  theme.paper_plot +
  coord_cartesian(clip = "off") +
  labs(x= "TPC-H Query", y="Norm. Runtime") +
  theme(legend.position=c(.14,.82)) +
  theme(legend.title = element_blank()) +
  theme(legend.background=element_blank()) +
  theme(legend.key.size = unit(4, "mm")) +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank()) +
  theme(plot.margin=unit(c(1,1,0,1), 'mm')) +
  theme(axis.title.y = element_text(hjust=0.7))
print(query_selection_argument)
ggsave("query_selection_argument.pdf", query_selection_argument, width=5, height=1.5)
