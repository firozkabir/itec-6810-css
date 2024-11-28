# -*- coding: utf-8 -*-
# Required packages
library(tidyverse) # metapackage of all tidyverse packages
library(ggplot2)

# install corrplot as it is usually not installed on colab
install.packages("corrplot")

# import corrplot
library(corrplot)

list.files(path = "data")

dat = read.csv("data/hour.csv", stringsAsFactors = FALSE)
names(dat)

# convert environmetal temperature in to general scale
dat$env.temp = dat$temp*(39+8) - 8
head(dat)
is.integer(dat)
is.null(dat)

# convert feel temperature to general scale
dat$feel.temp = dat$atemp*(50+16) - 16
summary(dat$feel.temp)

# convert humidity to general scale
dat$humidity = dat$hum*100
summary(dat$humidity)

# convert wind speed to general scale
#dat$wsp = dat$windspeed*67
dat$wsp = dat$windspeed*1
summary(dat$wsp)

#display count
options(repr.plot.width=12, repr.plot.height = 8)
hist(dat$cnt,  xlab = "no, of bicycles",ylab = "Frequency", main = "Histogram of total no. of bike rentals",
     col = '#2F4858', probability = F, cex.main = 2, cex.lab = 1.5, cex.axis = 1.5)
lines(density(dat$cnt))

summary(dat$cnt)

#creating the data frame
df = data.frame('lables' = c('Registered', 'Casual'), 'value' = c(mean(dat$registered)/(mean(dat$registered)+mean(dat$casual)),
                                                                  mean(dat$casual)/(mean(dat$registered)+mean(dat$casual))))
# Create a basic bar
pie = ggplot(df, aes(x="", y=value, fill=lables)) + geom_bar(stat="identity", width=1)
# Convert to pie (polar coordinates) and add labels
pie = pie + coord_polar("y", start=0) + geom_text(aes(label = paste0(round(value*100), "%")),
                                                  position = position_stack(vjust = 0.5), size = 10)
# Add color scale (hex colors)
pie = pie + scale_fill_manual(values=c("#F26419", "#F6AE2D", "#55DDE0", "#999999"))
# Remove labels and add title
pie = pie + labs(x = NULL, y = NULL, fill = NULL, title = "Rgistered vs. Casual Users")
# Tidy up the theme
pie = pie + theme_classic() + theme(text = element_text(size = 20),
          axis.line = element_blank(),
          axis.text = element_blank(),
          axis.ticks = element_blank(),
          plot.title = element_text(size = 24, hjust = 0.5, face="bold"))
pie

# registered and causal customers
options(repr.plot.width=10, repr.plot.height = 8)
boxplot(x=dat$registered,dat$casual,names = c("registered","casual"),col="#33658A",
        ylab="Bike rentals",xlab="Types of users", main = 'Bike rental vs. type of users',
        cex.main = 2, cex.lab = 1.5, cex.axis = 1.5)

# take the summary of environmetal temperature
# spring
spring = subset(dat, season == 1)$cnt
sp.sum = summary(spring)
# summer
summer = subset(dat, season == 2)$cnt
su.sum = summary(summer)
# fall
fall = subset(dat, season == 3)$cnt
f.sum = summary(fall)
# winter
winter = subset(dat, season == 4)$cnt
win.sum = summary(winter)

season_list = list(sp.sum, su.sum, f.sum, win.sum)
names(season_list) = c("spring", "summer", "fall", "winter")
season_list

scatter.smooth(dat$feel.temp,dat$cnt, col = "#33658A", xlab = "Feel temperature", ylab = "no. of bike rentals",
               main = "Bike rentals vs. feel temperature", cex.main = 2, cex.lab = 1.5, cex.axis = 1.3)
plot2 <- ggplot(dat, aes(feel.temp, cnt)) + geom_smooth(aes(color = cnt))
plot2 + xlab("Feel Temperature") + ylab("Average no. of Bike Rentals") +
    theme_light(base_size = 11) + scale_x_continuous(expand = c(0, 0)) + scale_y_continuous(expand = c(0, 0)) +
theme(text = element_text(size = 20), plot.title = element_text(size = 24, face="bold"))

# holiday vs count
tx = subset(dat, holiday == 0)
summary(tx$cnt)
ty = subset(dat, holiday == 1)
summary(ty$cnt)
boxplot(x=tx$cnt,ty$cnt,names = c("not holiday","holiday"),col="#33658A",
        ylab="Bike rentals",xlab="Types of day", main = "Bike rentals vs. holidays",
       cex.main = 2, cex.lab = 1.5, cex.axis = 1.3)

#working day vs count
tx = subset(dat, workingday == 0)
summary(tx$cnt)
ty = subset(dat, workingday == 1)
summary(ty$cnt)
boxplot(x=tx$cnt,ty$cnt,names = c("not workingday","workingday"),col="#999999",
        ylab="Bike rentals",xlab="Types of day", main = "Bike renals vs. working day",
       cex.main = 2, cex.lab = 1.5, cex.axis = 1.3)

# boxplot of rental season v.s. working day
ggplot(dat, aes(x = as.character(workingday) , y = cnt, fill = factor(season))) +
  geom_boxplot(outlier.color = adjustcolor("black", alpha.f = 1), na.rm = TRUE) +
  theme_light(base_size = 11) +
  xlab("Is it Workingday?") +
  ylab("Number of Bike Rentals") +
  ggtitle("Bike rentals of each season vs. Working day or not\n") +
  scale_fill_manual(values=c("#55DDE0",  "#F6AE2D", "#F26419", "#33658A", "#2F4858", "#999999"),
                    name="Season:",
                    breaks=c(1, 2, 3, 4),
                    labels=c("Spring", "Summer", "Fall","Winter")) +
  theme(text = element_text(size = 20), plot.title = element_text(size = 24, face="bold"))

#bicycles with humidity
hm = aggregate(cnt~humidity, mean, data = dat)
plot(hm$humidity, hm$cnt,  col = "#33658A", lwd = 2.5,
     xlab = "humidity", ylab = "no. of bicycles", main = "Average bike rentals vs. humidity",
    cex.main = 2, cex.lab = 1.5, cex.axis = 1.5, cex = 1.2)
# line plot of rental v.s. humidity
ggplot(dat, aes(x = humidity, y = cnt, color = weathersit)) +
  geom_smooth(method = 'auto', size = 1) +
  theme_light(base_size = 11) +
  xlab("Humidity") +
  ylab("Number of Bike Rentals") +
  ggtitle("\n") +
  theme(plot.title = element_text(size = 11, face="bold")) +
  theme(text = element_text(size = 20), plot.title = element_text(size = 24, face="bold"))

#bike rentas vs. wind speed
wsp = aggregate(cnt~wsp, mean, data = dat)
plot(wsp$wsp, wsp$cnt, col = "#33658A", lwd = 2.5,
     xlab = "wind speed(kmph)", ylab = "no. of bicycles", main = "Average bike rentals vs. wind speed",
    cex.main = 2, cex.lab = 1.5, cex.axis = 1.5, cex = 1.2)
# line plot of rental v.s. wind speed
ggplot(dat, aes(x = wsp, y = cnt), color = weathersit) +
 geom_smooth(method = 'auto', size = 1) +
 theme_light(base_size = 11) +
 xlab("Wind Speed") +
 ylab("Number of Bike Rentals") +
 ggtitle("\n") +
 theme(plot.title = element_text(size = 11, face="bold")) +
 theme(text = element_text(size = 20), plot.title = element_text(size = 24, face="bold"))

# count of bicylces with hour of the day
ch = aggregate(cnt~hr, mean, data = dat)
plot(ch$hr, ch$cnt, type = "b", col = "#33658A", lwd = 2.5, xlab = "hour of the day",
     ylab = "Average no. of bike rentals", main = "Bike rentals vs. hour of the day",
     cex.main = 2, cex.lab = 1.5, cex.axis = 1.3, cex = 1.2)

# line plot of rentals v.s. hour of day
ch = aggregate(cnt~hr+weekday, mean, data = dat)
ggplot(ch, aes(x = hr, y = cnt, color = as.factor(weekday))) +
  geom_line(size = 1) +
  theme_light(base_size = 11) +
  xlab("Hour of the Day") +
  ylab("Average no. of Bike Rentals") +
  ggtitle("Average bike rentals in each hour vs. day of the week\n") +
  scale_color_discrete("") + coord_cartesian(xlim = c(0,23), ylim = c(-5, 600)) +
  theme(text = element_text(size = 20), plot.title = element_text(size = 24, face="bold"))

# weather, holiday and bike retals
ggplot(dat, aes(x = as.character(holiday) , y = cnt, fill = factor(weathersit))) +
  geom_boxplot(outlier.color = adjustcolor("black", alpha.f = 1), na.rm = TRUE) +
  theme_light(base_size = 11) +
  xlab("Is it Holiday?") +
  ylab("Number of Bike Rentals") +
  ggtitle("Bike rentals on weather conditions vs. Holiday or not\n") +
  scale_fill_manual(values=c("#55DDE0",  "#F6AE2D", "#F26419", "#33658A", "#2F4858", "#999999"),
                    name="Weather:",
                    breaks=c(1, 2, 3, 4),
                    labels=c("Clear", "Mist", "Light Snow", "Heavy Rain")) +
  theme(text = element_text(size = 20), plot.title = element_text(size = 24, face="bold"))
