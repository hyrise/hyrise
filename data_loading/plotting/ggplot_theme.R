library(ggplot2)
library(ggthemes)

# http://blog.revolutionanalytics.com/2012/09/how-to-use-your-favorite-fonts-in-r-charts.html
theme.paper_plot <-
  theme(text = element_text(family="Times")) +
  theme(plot.title = element_text(face="bold")) +
  theme(panel.border = element_rect(colour = "black", fill=NA, size=.2)) +
  theme(strip.background = element_rect(color = "black", fill="#ededed", size=.4)) +
  theme(strip.text = element_text(face="bold")) # +
  # theme(panel.grid.major.x = element_blank(),
  #       panel.grid.major.y = element_line( size=.1, color="black" ))