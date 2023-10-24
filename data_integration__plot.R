library(dplyr)
library(ggplot2)
library(ggthemes)

if (Sys.getenv("RSTUDIO") == "1") {
  setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
}
print(getwd())
source("ggplot_theme.R")


results <- read.csv("/Users/martin/Programming/OpossumDB2/data_integration__loading_results.csv")
results$RUNTIME_S <- results$RUNTIME_US / 1000 / 1000
results$COLUMN_CONFIGURATION <- as.factor(results$COLUMN_CONFIGURATION)
results$RUN_CONFIG <- as.factor(results$RUN_CONFIG)
results$STEP <- as.factor(results$STEP)
results$SCALE_FACTOR <- as.factor(results$SCALE_FACTOR)

levels(results$COLUMN_CONFIGURATION) <- c("DBgen Modification:\nc_custkey &\n c_mktsegment",  "DBgen Modification:\nc_custkey only", "Post-DBgen Filtering:\nc_custkey &\n c_mktsegment" , "Post-DBgen Filtering:\nc_custkey only", "Default")
levels(results$RUN_CONFIG) <- c("", "Single-\nThreaded")
levels(results$STEP) <- c("#3 Statistic Generation", "#2 Data Encoding", "#1 Generating Table Data")
levels(results$SCALE_FACTOR) <- c("SF 10", "SF 30", "SF 60", "SF 100", "SF 200")

results_agg <- results %>% group_by(COLUMN_CONFIGURATION, SCALE_FACTOR, RUN_CONFIG, TABLE_NAME, STEP) %>%
                           summarize(RUNTIME_S_MEAN = mean(RUNTIME_S), .groups="keep")

results_fake <- results %>% group_by(COLUMN_CONFIGURATION, SCALE_FACTOR, RUN_CONFIG, TABLE_NAME, STEP) %>%
                            summarize(INTERMEDIATE = mean(RUNTIME_S), .groups="keep") %>%
                            group_by(COLUMN_CONFIGURATION, SCALE_FACTOR, RUN_CONFIG, TABLE_NAME) %>%
                            summarize(STEP_SUM = sum(INTERMEDIATE) * 1.2, .groups="keep")

plot <- function(df, df_fake, name) {
  g <- ggplot(df %>% filter(SCALE_FACTOR != "SF 1"),
              aes(x=RUN_CONFIG)) +
    geom_col(aes(y=RUNTIME_S_MEAN, fill=STEP)) +
    geom_point(data=df_fake, aes(y=STEP_SUM), alpha = 0.0) +
    theme_bw() +
    scale_colour_tableau(palette="Superfishel Stone") +
    scale_fill_tableau(palette="Superfishel Stone", name="Step:", guide = guide_legend(reverse=TRUE)) +
    theme.paper_plot +
    facet_grid(SCALE_FACTOR ~ COLUMN_CONFIGURATION, scales = "free_y") +
    stat_summary(fun = sum, aes(y = RUNTIME_S_MEAN, label = paste(round(after_stat(y), 2), "s"),
                                group = RUN_CONFIG), geom = "text", vjust = -.3, family="Times", size=3) +
    coord_cartesian(clip = "off") +
    labs(x= "Threading Configuration", y="Runtime [s]") +
    # scale_fill_discrete(name="Experimental\nCondition") +
    theme(legend.position="top")
  
  print(g)
  ggsave(name, g, width=5.25, height=6.0)
}

plot(results_agg, results_fake, "data_integration__loading.pdf")
plot(results_agg %>% filter(RUN_CONFIG == "" & !grepl("only", COLUMN_CONFIGURATION)),
     results_fake %>% filter(RUN_CONFIG == "" & !grepl("only", COLUMN_CONFIGURATION)), "data_integration__loading_simplified.pdf")