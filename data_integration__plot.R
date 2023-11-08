library(dplyr)
library(ggplot2)
library(ggthemes)
library(stringr)

if (Sys.getenv("RSTUDIO") == "1") {
  setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
}
print(getwd())
source("ggplot_theme.R")


results <- read.csv("data_integration__loading_results_q3.csv")
results$RUNTIME_S <- results$RUNTIME_US / 1000 / 1000
results$COLUMN_CONFIGURATION <- as.factor(results$COLUMN_CONFIGURATION)
results$RUN_CONFIG <- as.factor(results$RUN_CONFIG)
results$STEP <- as.factor(results$STEP)
results$SCALE_FACTOR <- as.factor(results$SCALE_FACTOR)
results$RUNTIME_S <- results$RUNTIME_S / 10  # we run queries 10 times (11 with warmup)

levels(results$COLUMN_CONFIGURATION) <- list("DBgen Modification:\ngenerate only accessed columns" = "Q3_COLUMNS",
                                             "Post-DBgen Filtering:\ngenerate only accessed columns" = "DB_Q3_COLUMNS",
                                             "Hyrise Default:\ngenerate and load all columns" = "NONE",
                                             "DBgen Default:\n.tbl file creation & loading" = "CSV")
levels(results$RUN_CONFIG) <- c("", "Single-\nThreaded")
levels(results$STEP) <- c("#3 Statistic Generation", "#2 Data Encoding", "#1 Generating Table Data", "#4 Query")

results_agg <- results %>% filter(STEP != "#4 Query") %>%
                           group_by(COLUMN_CONFIGURATION, SCALE_FACTOR, RUN_CONFIG, STEP) %>%
                           summarize(RUNTIME_S_MEAN = mean(RUNTIME_S), .groups="keep")

results_fake <- results %>% filter(STEP != "#4 Query") %>%
                            group_by(COLUMN_CONFIGURATION, SCALE_FACTOR, RUN_CONFIG, STEP) %>%
                            summarize(INTERMEDIATE = mean(RUNTIME_S), .groups="keep") %>%
                            group_by(COLUMN_CONFIGURATION, SCALE_FACTOR, RUN_CONFIG) %>%
                            summarize(STEP_SUM = sum(INTERMEDIATE) * 1.2, .groups="keep")

query_runtimes_debug <- results %>% filter(STEP == "#4 Query") %>%
                                    group_by(SCALE_FACTOR, COLUMN_CONFIGURATION, RUN_CONFIG) %>%
                                    summarize(MIN_RUNTIME = min(RUNTIME_S),
                                              MAX_RUNTIME = max(RUNTIME_S),
                                              MEAN_RUNTIME = mean(RUNTIME_S),
                                              MEDINA_RUNTIME = median(RUNTIME_S), .groups="keep") %>%
                                    mutate(SCALE_FACTOR_RUNTIME_LABEL = paste0("SF ", SCALE_FACTOR, " (", round(MEAN_RUNTIME, 2), " s)"))

query_runtimes <- results %>% filter(STEP == "#4 Query") %>%
                                    filter(RUN_CONFIG == "") %>%
                                    group_by(SCALE_FACTOR, RUN_CONFIG) %>%
                                    summarize(MEAN_RUNTIME = mean(RUNTIME_S), .groups="keep") %>%
                                    mutate(SCALE_FACTOR_RUNTIME_LABEL = paste0("SF ", SCALE_FACTOR, " (", round(MEAN_RUNTIME, 2), " s)"))


results_agg <- results_agg %>% inner_join(query_runtimes, by = c("SCALE_FACTOR", "RUN_CONFIG"))
results_fake <- results_fake %>% inner_join(query_runtimes, by = c("SCALE_FACTOR", "RUN_CONFIG"))

ggplot(results %>% filter(RUN_CONFIG == ""),
       aes(x=STEP)) +
  geom_hline(data = query_runtimes, aes(yintercept = MEAN_RUNTIME)) +
  geom_boxplot(aes(y=RUNTIME_S, fill=STEP)) +
  theme_bw() +
  scale_colour_tableau(palette="Superfishel Stone") +
  scale_fill_tableau(palette="Superfishel Stone", name="Step:", guide = guide_legend(reverse=TRUE)) +
  theme.paper_plot +
  facet_wrap(SCALE_FACTOR ~ COLUMN_CONFIGURATION, ncol=n_distinct(results$COLUMN_CONFIGURATION), scales = "free") +
  # stat_summary(fun = sum, aes(y = RUNTIME_S_MEAN, label = paste(round(after_stat(y), 2), "s"),
  #                             group = RUN_CONFIG), geom = "text", vjust = -0.5, family="Times", size=3) +
  # stat_summary(fun.data = mean_se, aes(y = RUNTIME_US, group = RUN_CONFIG), geom = "errorbar", position = "dodge") +
  coord_cartesian(clip = "off") +
  labs(x= "Threading Configuration", y="Runtime [s]") +
  theme(legend.position="top") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))

ggplot(results %>% filter(RUN_CONFIG == "" & STEP == "#4 Query"),
       aes(x=STEP)) +
  geom_hline(data = query_runtimes, aes(yintercept = MEAN_RUNTIME)) +
  geom_boxplot(aes(y=RUNTIME_S, fill=STEP)) +
  theme_bw() +
  scale_colour_tableau(palette="Superfishel Stone") +
  scale_fill_tableau(palette="Superfishel Stone", name="Step:", guide = guide_legend(reverse=TRUE)) +
  theme.paper_plot +
  facet_grid(SCALE_FACTOR ~ COLUMN_CONFIGURATION, scales = "free_y") + # wrap: ncol=n_distinct(results$COLUMN_CONFIGURATION), 
  # stat_summary(fun = sum, aes(y = RUNTIME_S_MEAN, label = paste(round(after_stat(y), 2), "s"),
  #                             group = RUN_CONFIG), geom = "text", vjust = -0.5, family="Times", size=3) +
  # stat_summary(fun.data = mean_se, aes(y = RUNTIME_US, group = RUN_CONFIG), geom = "errorbar", position = "dodge") +
  coord_cartesian(clip = "off") +
  labs(x= "Threading Configuration", y="Runtime [s]") +
  theme(legend.position="top") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))

results_agg <- results_agg %>% mutate(SCALE_FACTOR_RUNTIME_LABEL = factor(SCALE_FACTOR_RUNTIME_LABEL, stringr::str_sort(unique(SCALE_FACTOR_RUNTIME_LABEL), numeric = TRUE)))
results_fake <- results_fake %>% mutate(SCALE_FACTOR_RUNTIME_LABEL = factor(SCALE_FACTOR_RUNTIME_LABEL, stringr::str_sort(unique(SCALE_FACTOR_RUNTIME_LABEL), numeric = TRUE)))

plot <- function(df, df_fake, name) {
  g <- ggplot(df, # %>% filter(SCALE_FACTOR != "SF 1"),
              aes(x=RUN_CONFIG)) +
    geom_hline(data = query_runtimes, aes(yintercept = MEAN_RUNTIME)) +
    geom_col(aes(y=RUNTIME_S_MEAN, fill=STEP)) +
    geom_point(data=df_fake, aes(y=STEP_SUM), alpha = 0.0) +
    theme_bw() +
    scale_colour_tableau(palette="Superfishel Stone") +
    scale_fill_tableau(palette="Superfishel Stone", name="Step:", guide = guide_legend(reverse=TRUE)) +
    theme.paper_plot +
    facet_grid(SCALE_FACTOR_RUNTIME_LABEL ~ COLUMN_CONFIGURATION, scales = "free_y") +
    stat_summary(fun = sum, aes(y = RUNTIME_S_MEAN, label = paste(round(after_stat(y), 2), "s"),
                                group = RUN_CONFIG), geom = "text", vjust = -.3, family="Times", size=3) +
    coord_cartesian(clip = "off") +
    labs(x= "Threading Configuration", y="Runtime [s]") +
    theme(legend.position="top")
  
  print(g)
  ggsave(name, g, width=5.25, height=6.0)
}

plot(results_agg, results_fake, "data_integration__loading_q3.pdf")
plot(results_agg %>% filter(RUN_CONFIG == ""),
     results_fake %>% filter(RUN_CONFIG == ""), "data_integration__loading_simplified_q3.pdf")
