library(Cairo)
library(extrafont)
library(ggplot2)
library(ggthemes)

# Linux Libertine
#  Use TTF ones from here: https://fontlibrary.org/en/font/linux-libertine
# font_import()

loadfonts(quiet=TRUE)

# http://blog.revolutionanalytics.com/2012/09/how-to-use-your-favorite-fonts-in-r-charts.html
theme.paper_plot <-
  theme(text = element_text(family="Linux Libertine")) +
  # theme(text = element_text(family="Times")) +
  theme(plot.title = element_text(face="bold")) +
  theme(panel.border = element_rect(colour = "black", fill=NA, linewidth=.2)) +
  theme(strip.background = element_rect(color = "black", fill="#ededed", linewidth=.4)) +
  theme(strip.text = element_text(face="bold")) # +
  # theme(panel.grid.major.x = element_blank(),
  #       panel.grid.major.y = element_line( size=.1, color="black" ))