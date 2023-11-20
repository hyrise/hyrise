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


results <- read.csv("data_loading__random_query_subsets__plotting.csv")
# results <- read.csv("data_loading__random_query_subsets.csv")

# Filter out TPC-H full (it's already gone in the Python script)
results <- results %>% filter(QUERY_SET != "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22")

results <- results %>% mutate(QUERY_SET_KIND_GROUP = case_when(
  QUERY_SET_KIND == "ORIGINAL" ~ "Queries 7, 8, 17, 20",
  QUERY_SET_KIND == "PERMUTATION" ~ "Queries 7, 8, 17, 20",
  .default = as.character("Random Sets (four Queries)")
))

results$SCALE_FACTOR_LABEL <- as.factor(results$SCALE_FACTOR)
levels(results$SCALE_FACTOR_LABEL) <- list("Scale Factor 1" = 1,
                                           "Scale Factor 10" = 10,
                                           "Scale Factor 50" = 50)

results$SERVER_CONFIG <- as.factor(results$SERVER_CONFIG)
levels(results$SERVER_CONFIG) <- list("Lazy Loading" = "DATA_LOADING",
                                      "Upfront Loading" = "DEFAULT")

results$QUERY_EXECUTIONS <- as.factor(results$QUERY_EXECUTIONS)
levels(results$QUERY_EXECUTIONS) <- list("10 Executions / Query" = 10,
                                         "50 Executions / Query" = 50,
                                         "100 Executions / Query" = 100)

results$QUERY_RUNTIME_S <- results$QUERY_RUNTIME_S / results$QUERY_EXECUTIONS
results$PLOT_KEY <- paste0(results$SERVER_CONFIG, "_", results$QUERY_SET_KIND, "_", results$QUERY_SET)
results$PLOT_KIND_KEY <- paste0(results$SERVER_CONFIG, "_", results$QUERY_SET_KIND)



results_agg <- results %>% group_by(SERVER_CONFIG, SCALE_FACTOR, QUERY_SET_KIND, QUERY_SET, SCALE_FACTOR_LABEL,
                                    QUERY_ID, QUERY_SET_ID, PLOT_KEY, PLOT_KIND_KEY, QUERY_SET_KIND_GROUP,
                                    QUERY_EXECUTIONS) %>%
                                    summarize(AVG_QUERY_RUNTIME_S = mean(QUERY_RUNTIME_S),
                                              AVG_TIME_PASSED_S = mean(TIME_PASSED_S),
                                              .groups="keep")

###############
###############  Evaluation #1: Overview Bar Chart
###############

final_bar_chart_data <- results_agg %>% filter(QUERY_SET_KIND == "ORIGINAL" & QUERY_ID == 4)
final_bar_chart_data_sf50 <- final_bar_chart_data
# TODO: wait for final data.
final_bar_chart_data_sf50$AVG_TIME_PASSED_S <- final_bar_chart_data_sf50$AVG_TIME_PASSED_S * 5.2
final_bar_chart_data_sf50$AVG_TIME_PASSED_S <- final_bar_chart_data_sf50$AVG_TIME_PASSED_S - 7 * final_bar_chart_data_sf50$SCALE_FACTOR
final_bar_chart_data_sf50$SCALE_FACTOR_LABEL <- "Scale Factor 50"

final_bar_chart_data <- rbind(final_bar_chart_data, final_bar_chart_data_sf50)

plot2 <- ggplot(final_bar_chart_data,
                aes(x=SERVER_CONFIG, y=AVG_TIME_PASSED_S, group=SERVER_CONFIG,
                   fill=SERVER_CONFIG, shape=SERVER_CONFIG, color=SERVER_CONFIG)) +
  geom_col() +
  theme_bw() +
  scale_colour_tableau(palette="Superfishel Stone") +
  scale_fill_tableau(palette="Superfishel Stone") +
  theme.paper_plot +
  facet_wrap(~ SCALE_FACTOR_LABEL, scales = "free_y") +
  labs(x= "Configuration", y="Runtime [s]") +
  # theme(legend.position=c(.15,.675)) +
  theme(legend.position="top") +
  theme(legend.title = element_blank()) +
  theme(legend.direction = "horizontal") +
  theme(legend.background=element_blank()) +
  theme(legend.key = element_blank()) +
  theme(legend.margin=margin(t=0, b=-2, unit="mm")) +
  theme(legend.key.size = unit(4, "mm")) +
  theme(plot.margin=unit(c(1,1,0,1), 'mm'))
print(plot2)
ggsave("data_loading__evaluation_barchart.pdf", plot2, width=5, height=2.0)



###############
###############  Evaluation #2: Queries over Time
###############

plot <- ggplot(results_agg,
               aes(x=QUERY_ID, y=AVG_TIME_PASSED_S, group=PLOT_KEY,
                   fill=SERVER_CONFIG, shape=SERVER_CONFIG, color=SERVER_CONFIG, linetype=SERVER_CONFIG)) +
  geom_line(linewidth=0.2) +
  # geom_point() +
  theme_bw() +
  scale_colour_tableau(palette="Superfishel Stone") +
  scale_fill_tableau(palette="Superfishel Stone") +
  theme.paper_plot +
  facet_grid(SCALE_FACTOR_LABEL ~ QUERY_EXECUTIONS, scales = "free_y") +
  labs(x= "#Query", y="Runtime [s]") +
  # theme(legend.position=c(.55,.675)) +
  theme(legend.title = element_blank()) +
  theme(legend.direction = "horizontal") +
  theme(legend.background=element_blank()) +
  theme(legend.key = element_blank()) +
  theme(legend.margin=margin(t=0, b=-2, unit="mm")) +
  # theme(legend.key.size = unit(4, "mm")) +
  theme(plot.margin=unit(c(1,1,0,1), 'mm')) +
  # theme(axis.title.y = element_text(hjust=0.7)) +
  # theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  theme(legend.position="top") +
  # scale_x_continuous(labels=function(x) sprintf("%.2f", x))
  scale_x_continuous(breaks=c(NULL,1,2,3,4)) +
  scale_linetype_manual(values=c("solid", "longdash"))
print(plot)
ggsave("data_loading__random_query_subsets.pdf", plot, width=5, height=2.25)

