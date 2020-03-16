# Header ---------------------------------------------




# Libraries and working directory ---------------------------------------------

# Dummy path
setwd(path)

library(tidyverse)
library(mlbench)
library(caret)
library(timeDate)
library(MLmetrics) # MAPE, R2_Score, function
library(ranger) # Random forest
library(gbm) # Gradient boosting machine
library(xgboost) # Another gradient boosting machine
library(glmnet) # Elastic net 
library(data.table)
# caretEnsemble # For ensemble models?

# Load data ---------------------------------------------

load("gas_burn_data_3.RData")

# Reformat the dataset  ---------------------------------------------

# Create subet - excludes "lgCGL1","lgCGL2","lgCGL7", "day.1"
data <- data[, .(burn, burnl1, burnl2, burnl7,  loadl1, 
                 loadl2, loadl7, dasolar, onifml1, onifml2, onifml7, offifml1, offifml2, 
                 offifml7, sysloadl1, sysloadl2, sysloadl7, peakisol1, 
                 peakisol2, peakisol7, avgtheoutl1, avgtheoutl2, avgtheoutl7, importsl1, importsl2,
                 importsl7, week, month, index, weekend, holiday)]

# Formula to feed into the machine learning models below 
# There is something wrong with this formula and how it is being interpreted by glmnet
formula <- data$burn ~ data$burnl1 + data$burnl2 + data$burnl7 + data$loadl1 + 
  data$loadl2 + data$loadl7 + data$dasolar +data$onifml1 + data$onifml2 + data$onifml7 + data$offifml1 + data$offifml2 +
  data$offifml7 + data$sysloadl1 + data$sysloadl2 + data$sysloadl7 + data$loadl1 + data$loadl2 + data$loadl7 + data$peakisol1 +
  data$peakisol2 + data$peakisol7 + data$avgtheoutl1 + data$avgtheoutl2 +  data$avgtheoutl7 + data$importsl1 + data$importsl2 +
  data$importsl7 + data$week + data$month + data$index + data$weekend + data$holiday


# Prepare the parameters and settings for the models ---------------------------------------------

# Create the hyperparameter grid for the elastic net regression - see potential values of lamda
# These are the suggested values of alpha and lamda
# Alpha = 1 means Lasso regression 
# Alpha = 0 means Ridge regression
glmnet_grid <- expand.grid(alpha = seq(0, 1, length = 10), lambda = seq(0.0001, 0.1, length = 100))

# Specify how missing values are to be treated - assume median imputation
# General suggestion is to use median imputation first, but knn is another popular method
preProces_glm <- c("medianImpute", "center", "scale")
preProces_other <- c("medianImpute")


# An integer denoting the amount of granularity in the tuning parameter grid
tune_len <- 3

# rf_grid <- expand.grid(num.trees = c(5, 10, 15, 20, 25, 30, 35, 40, 45, 50),
                      # mtry = c(3,5))


# Specify the settings of the train function -5X2 cross-validation
# summaryFunction = model.score, # This was at the top before 
my_control <- trainControl(#summaryFunction = model.score, - couldn't get custom calcs working
                           method = "cv", # Cross validation method
                           number = 10, # Number of fold (k-fold)
                           repeats = 2, # Number of times to repeat the CV
                           verboseIter = TRUE, # Print progress log as models are fit
                           savePredictions = TRUE,
                           search = "random") # Save all hold out predictions for each resample


# Run the Elastic Net regression  ---------------------------------------------

# Select subset for test
#data <- data[, 1:10]

# Set seed for controlled randomness
set.seed(666)

# Convert back to data.frame
data <- as.data.frame(data)

# Elastic Net Regression
# Acceptable to include categorical as integer
model_glmnet <- train(
  burn ~ .,
  data = data,
  method = "glmnet",
  na.action = na.pass, 
  preProcess = preProces_glm,
  tuneGrid = glmnet_grid,
  trControl = my_control
)

# Random Forest
model_rf <- train(
  burn ~ .,
  data,
  method = "ranger",
  preProcess = preProces_other, 
  na.action = na.pass,
  tuneLength = tune_len,
  # tuneGrid = rf_grid,
  num.trees = 100,
  trControl = my_control
)

# Look at the Random Forest results
plot(model_rf)
model_rf$results

# Create a list out of the model
model_list <- list(glmnet = model_glmnet, random_forest = model_rf)

# Resamples is a function for collecting the output from multiple models 
resamps <- resamples(model_list)

# Produce graphs
dotplot(resamps, metric = "Rsquared")
xyplot(resamps)
bwplot(resamps)

# Alec's function currently not functional ---------------------------------------------

# # Alec's model score
#model.score <- function(data, lev = NULL, model = NULL) {
#pred <- exp(data$pred)
#obs <- exp(data$obs)
#mape_val <- MAPE(y_pred = pred, y_true = obs)
#R2_val <- R2_Score(y_pred = pred, y_true = obs)
#c(R2.val = R2_val, MAPE.val = -mape_val, RMSE = RMSE_val)
#}




