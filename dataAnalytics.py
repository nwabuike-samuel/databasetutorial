# %%
# import used libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, StandardScaler
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error
from math import sqrt

# %%
# Import CarSharing into pandas dataframe
df = pd.read_csv('CarSharing.csv')
df.head

# %%
df.info()

# %%
#drop na
df.dropna(inplace=True)
#drop duplicates
df.drop_duplicates(inplace=True)
# change timestamp to date type
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.info()

# %%
#Encoding the categorical variables

le = LabelEncoder()
df['season'] = le.fit_transform(df['season'])
df['holiday']=le.fit_transform(df['holiday'])
df['workingday'] = le.fit_transform(df['workingday'])
df['weather'] = le.fit_transform(df['weather'])

df.head()

# %%
df.info()

# %%
# oe = OneHotEncoder(sparse=False)
# df['workingday'] = oe.fit_transform(df['workingday'].values.reshape(-1,1))
# df['holiday'] = oe.fit_transform(df['holiday'].values.reshape(-1,1))
# df['weather_code'] = oe.fit_transform(df['weather_code'].values.reshape(-1,1))

# %% [markdown]
# ### Question 2

# %%
from sklearn.feature_selection import f_regression
from sklearn.feature_selection import SelectKBest

# %% [markdown]
# ### Linear Regression

# %%
from sklearn.linear_model import LinearRegression
model = LinearRegression()
from scipy.stats import linregress

# %%
# temp and demand
x = df['temp'].values.reshape(-1, 1)
y = df['demand'].values.reshape(-1, 1)

model.fit(x,y)
coeff = model.coef_[0][0]
p_value = linregress(x.flatten(), y.flatten())[3]

print("p_value: ", p_value)
print("regression coeff: ", coeff)

# %%
# humidity and demand
x = df['humidity'].values.reshape(-1, 1)
y = df['demand'].values.reshape(-1, 1)

model.fit(x,y)
coeff = model.coef_[0][0]
p_value = linregress(x.flatten(), y.flatten())[3]
print("p_value: ", p_value)
print("regression coeff: ", coeff)

# %%
# windspeed and demand
x = df['windspeed'].values.reshape(-1, 1)
y = df['demand'].values.reshape(-1, 1)

model.fit(x,y)
coeff = model.coef_[0][0]
p_value = linregress(x.flatten(), y.flatten())[3]
print("p_value: ", p_value)
print("regression coeff: ", coeff)

# %% [markdown]
# ### Z test
# 
# 
# 

# %%
# workingday and demand
import scipy.stats as stats
group0 = df.loc[df['workingday'] == 0, 'demand']
group1 = df.loc[df['workingday'] == 1, 'demand']

print(np.var(group0, ddof=1), np.var(group1, ddof=1))

def f_test(group0, group1):
    f = np.var(group0, ddof=1) / np.var(group1, ddof=1)
    nun = group0.size - 1
    dun = group1.size - 1
    p_value = 1 - stats.f.cdf(f, nun, dun)
    return f, p_value

f, p = f_test(group0, group1)
print('F-test: F =', f, 'p-value =', p)

t, p = stats.ttest_ind(a=group0, b=group1, equal_var=True)
print('t-test: t =', t, 'p-value =', p)

# %% [markdown]
# ### Question 3

# %%
timestamp = df[df['timestamp'].dt.year == 2017]['timestamp']
temp = df[df['timestamp'].dt.year == 2017]['temp']
temp.index = timestamp
temp_weekly_average = temp.resample('W').mean()
temp_weekly_average.dropna(inplace = True)

# Split training and test sets
train_size = int(len(temp_weekly_average) * 0.7)
train, test = temp_weekly_average.iloc[:train_size], temp_weekly_average.iloc[train_size:]

history = [x for x in train]
predictions = list()
# walk-forward validation
for t in range(len(test)):
    model = ARIMA(history, order=(0,0,1))
    model_fit = model.fit()
    output = model_fit.forecast()
    yhat = output[0]
    predictions.append(yhat)
    obs = test[t]
    history.append(obs)
    print('predicted=%f, expected=%f' % (yhat, obs))

# # Fit ARIMA model to training data
# model = ARIMA(train, order=(5, 1, 0))
# model.fit = model.fit()

# # Predict
# pred = model.fit.forecast(steps=len(test))[0]

# Calculate RMSE
rmse = sqrt(mean_squared_error(test, predictions))
print('Test RMSE: %3f' % rmse)

# Plot actual and predicted values
plt.plot(test.index, test.values, label='Actual')
plt.plot(test.index, predictions, color='red', label='Predicted')
plt.legend()
plt.show()

# %% [markdown]
# ### Question 4

# %%
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score

# %%
#  Filter the dataset to include only the data for the year 2017
df_2017 = df[df['timestamp'].dt.year == 2017]

# Split the dataset into training and test sets
X = df_2017[['temp', 'humidity', 'windspeed', 'season']]
y = df_2017['weather']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

num_cols = ['temp', 'humidity', 'windspeed']
cat_cols = ['season']

scaler = StandardScaler()
X_train[num_cols] = scaler.fit_transform(X_train[num_cols])
X_test[num_cols] = scaler.transform(X_test[num_cols])

X_train_final = pd.concat([X_train[cat_cols], X_train[num_cols]], axis=1)
X_test_final = pd.concat([X_test[cat_cols], X_test[num_cols]], axis=1)

# %% [markdown]
# #### Using Random Forest Classifier

# %%
rfc = RandomForestClassifier()
rfc.fit(X_train_final, y_train)
y_pred = rfc.predict(X_test_final)
rfc_accuracy = accuracy_score(y_test, y_pred)

# %% [markdown]
# #### Using Support Vector MAchines

# %%
svc = SVC()
svc.fit(X_train_final, y_train)
y_pred = svc.predict(X_test_final)
svc_accuracy = accuracy_score(y_test, y_pred)

# %% [markdown]
# #### Using Gradient Boosting Classifier
# 

# %%
gbc = GradientBoostingClassifier()
gbc.fit(X_train_final, y_train)
y_pred = gbc.predict(X_test_final)
gbc_accuracy = accuracy_score(y_test, y_pred)

# %% [markdown]
# #### Models Accuracy

# %%
print("Random Forest Classifier Accuracy: ", rfc_accuracy)
print("Support Vector Machines Accuracy: ", svc_accuracy)
print("Gradient Boosting Classifier Accuracy: ", gbc_accuracy)

# %% [markdown]
# ### Question 5

# %%
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestRegressor

# %%
X.info()

# %%
# Split the data into predictor features (X) and target variable (y)
X = df_2017[['temp', 'humidity', 'windspeed', 'season']]
y = df_2017['demand']

# Split the data into train and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Define the numerical and categorical columns
num_cols = ['temp', 'humidity', 'windspeed']
cat_cols = ['season']

# Scale the numerical features using StandardScaler
scaler = StandardScaler()
X_train[num_cols] = scaler.fit_transform(X_train[num_cols])
X_test[num_cols] = scaler.transform(X_test[num_cols])

# One-hot encode the categorical feature using pd.get_dummies() function
X_train_final = pd.concat([pd.get_dummies(X_train[cat_cols]), X_train[num_cols]], axis=1)
X_test_final = pd.concat([pd.get_dummies(X_test[cat_cols]), X_test[num_cols]], axis=1)


# %%
mlp = MLPRegressor(max_iter=1000)

parameters = {
    'hidden_layer_sizes': [(10,), (20,), (30,), (40,), (50,), (60,)],
    'activation': ['relu', 'tanh'],
}

# %%
clf = GridSearchCV(mlp, parameters, cv=5, verbose=3)
clf.fit(X_train, y_train)

# %%
# Train and evaluate the deep neural network model with the best hyperparameters
mlp_best = MLPRegressor(max_iter=1000, **clf.best_params_)
mlp_best.fit(X_train, y_train)
y_pred_mlp = mlp_best.predict(X_test)
mlp_mse = mean_squared_error(y_test, y_pred_mlp)

# %%
# Train and evaluate the random forest regressor model
rf = RandomForestRegressor(n_estimators=100)
rf.fit(X_train, y_train)
y_pred_rf = rf.predict(X_test)
rf_mse = mean_squared_error(y_test, y_pred_rf)

# %%
print("Minimum Mean Squared Error (Deep Neural Network): ", mlp_mse)
print("Minimum Mean Squared Error (Random Forest Regressor): ", rf_mse)

# %% [markdown]
# The model with the lower mean squared error is working better.

# %% [markdown]
# ### Question 6

# %%
from sklearn.cluster import KMeans

# %%
X = df_2017[['humidity']]

# %% [markdown]
# #### Use Elbow Method to find the appropriate number of clusters

# %%
wcss=[]
for i in range(1,11):
  kmeans = KMeans(n_clusters=i, init='k-means++', random_state=42)
  kmeans.fit(X)
  wcss.append(kmeans.inertia_)

plt.plot(range(1, 11), wcss)
plt.title('Elbow Method')
plt.xlabel('Number of clusters')
plt.ylabel('WCSS')
plt.show()

# %% [markdown]
# As shown in the graph above, the elbow point is 3. Therefore, the appropriate number of clusters to use is 3.

# %%



