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


# results <- read.csv("../results/data_loading__random_query_subsets__plotting.csv")
results <- read.csv("../results/data_loading__random_query_subsets.csv")

results <- data.frame()
for (scale_factor in c(1, 10, 50)) {
  for (query_set in c("JCCHEVALSET", "ALLTPCH", "JCCHVARIANTS", "RANDOMVARIANTS")) {
    for (query_set_size in c(100)) {
      for (run_count in c(1, 10, 100)) {
        filename <- paste0("../results/data_loading__results_sf", scale_factor, "__", run_count, "runs__set_", query_set, "__", query_set_size, "perms.csv")
        
        if (!file.exists(filename)) {
          next
        }
        results_to_append <- read.csv(filename)
        results <- rbind(results, results_to_append)
      }
    }
  }
}

results <- results %>% group_by(SERVER_CONFIG, QUERY_SET_KIND, QUERY_SET,
                                     SCALE_FACTOR, QUERY_SET_ID, QUERY_EXECUTIONS) %>%
                            filter(max(QUERY_ID) == 4)

# Filter out TPC-H full
results <- results %>% filter(QUERY_SET != "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22")

results <- results %>% mutate(QUERY_SET_KIND_GROUP = case_when(
  QUERY_SET_KIND == "ORIGINAL" ~ "Queries 7, 8, 17, 20",
  QUERY_SET_KIND == "PERMUTATION" ~ "Queries 7, 8, 17, 20",
  .default = as.character("Random Sets (four Queries)")
))

results$SCALE_FACTOR_LABEL <- as.factor(results$SCALE_FACTOR)
levels(results$SCALE_FACTOR_LABEL) <- list("Scale Factor 1" = 1,
                                           "Scale Factor 10" = 10,
                                           "Scale Factor 50" = 50,
                                           "Scale Factor 100" = 100)

results$SERVER_CONFIG <- as.factor(results$SERVER_CONFIG)
levels(results$SERVER_CONFIG) <- list("Lazy Loading" = "DATA_LOADING",
                                      "Upfront Loading" = "DEFAULT")

results$QUERY_EXECUTIONS_STR <- as.factor(results$QUERY_EXECUTIONS)
levels(results$QUERY_EXECUTIONS_STR) <- list("1 Execution / Query" = 1,
                                             "10 Executions / Query" = 10,
                                             "50 Executions / Query" = 50,
                                             "100 Executions / Query" = 100)

results$QUERY_RUNTIME_S <- results$QUERY_RUNTIME_S / results$QUERY_EXECUTIONS
results$PLOT_KEY <- paste0(results$SERVER_CONFIG, "_", results$QUERY_SET_KIND, "_", results$QUERY_SET)
results$PLOT_KIND_KEY <- paste0(results$SERVER_CONFIG, "_", results$QUERY_SET_KIND)



results_agg <- results %>% group_by(SERVER_CONFIG, SCALE_FACTOR, QUERY_SET_KIND, QUERY_SET, SCALE_FACTOR_LABEL,
                                    QUERY_ID, QUERY_SET_ID, PLOT_KEY, PLOT_KIND_KEY, QUERY_SET_KIND_GROUP,
                                    QUERY_EXECUTIONS, QUERY_EXECUTIONS_STR) %>%
                                    summarize(AVG_QUERY_RUNTIME_S = mean(QUERY_RUNTIME_S),
                                              AVG_TIME_PASSED_S = mean(TIME_PASSED_S),
                                              .groups="keep")

###############
###############  Evaluation #1: Overview Bar Chart
###############

final_bar_chart_data <- results_agg %>% filter(QUERY_SET_KIND == "ORIGINAL" & QUERY_ID == 4)

plot2 <- ggplot(final_bar_chart_data %>% filter(QUERY_EXECUTIONS == 10),
                aes(x=SERVER_CONFIG, y=AVG_TIME_PASSED_S, group=SERVER_CONFIG,
                   fill=SERVER_CONFIG, shape=SERVER_CONFIG, color=SERVER_CONFIG)) +
  geom_col() +
  geom_text(aes(y=AVG_TIME_PASSED_S/2, label=paste(round(AVG_TIME_PASSED_S, 1), "s")),
            color="white", family="Times", size=3) +
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
###############  Evaluation #2: JCCH-Eval Queries over Time
###############

jcch_eval_set_results <- results_agg %>% filter(QUERY_SET_KIND_GROUP == "Queries 7, 8, 17, 20") #%>% 
                                         #filter(SCALE_FACTOR == 100)

plot <- ggplot(jcch_eval_set_results,
               aes(x=QUERY_ID, y=AVG_TIME_PASSED_S, group=PLOT_KEY,
                   fill=SERVER_CONFIG, shape=SERVER_CONFIG, color=SERVER_CONFIG, linetype=SERVER_CONFIG)) +
  geom_line(linewidth=0.2) +
  # geom_point() +
  theme_bw() +
  scale_colour_tableau(palette="Superfishel Stone") +
  scale_fill_tableau(palette="Superfishel Stone") +
  theme.paper_plot +
  facet_wrap(SCALE_FACTOR_LABEL ~ QUERY_EXECUTIONS_STR, scales = "free_y") +
  labs(x= "#Query", y="Time Passed [s]") +
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
ggsave("data_loading__evaluation_query_set.pdf", plot, width=5, height=3.5)




###############
###############  Evaluation #3: Random TPC-H Queries over Time
###############

random_set_results <- results_agg %>% filter(QUERY_SET_KIND_GROUP == "Random Sets (four Queries)") %>% 
                                      #filter(SCALE_FACTOR == 50)
                                      filter(SCALE_FACTOR == 10)

random_set_results_selection <- random_set_results %>% group_by(PLOT_KEY, SERVER_CONFIG, QUERY_EXECUTIONS, SCALE_FACTOR) %>%
                                                       mutate(query_set_runtime = max(AVG_TIME_PASSED_S))

random_set_results_selection <- random_set_results_selection %>% group_by(SERVER_CONFIG, QUERY_EXECUTIONS, SCALE_FACTOR) %>%
                                                                 mutate(is_slowest = query_set_runtime == max(query_set_runtime),
                                                                        is_fastest = query_set_runtime == min(query_set_runtime))

random_set_results_selection <- random_set_results_selection %>% filter(is_fastest == TRUE | is_slowest == TRUE)

plot <- ggplot(random_set_results,
               aes(x=QUERY_ID, y=AVG_TIME_PASSED_S, group=PLOT_KEY,
                   fill=SERVER_CONFIG, shape=SERVER_CONFIG, color=SERVER_CONFIG)) + #, linetype=SERVER_CONFIG
  geom_line(linewidth=0.2) +
  geom_line(data=random_set_results_selection, linewidth=2, aes(color=SERVER_CONFIG)) +
  # geom_point() +
  theme_bw() +
  scale_colour_tableau(palette="Superfishel Stone") +
  scale_fill_tableau(palette="Superfishel Stone") +
  theme.paper_plot +
  facet_wrap(SCALE_FACTOR_LABEL ~ QUERY_EXECUTIONS_STR, scales = "free_y") +
  labs(x= "#Query", y="Time Passed [s]") +
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
ggsave("data_loading__random_query_subsets.pdf", plot, width=5, height=3.5)

