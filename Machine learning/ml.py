# import libraries
import pandas as pd 
import psycopg2


## Extract Clean Data

# connection to postgres using sqlalchemy
conn = psycopg2.connect(host="postgres-ds9", database="postgres", user="postgres", password="password")
cursor = conn.cursor()

# extract train data
application_train_clean = pd.read_sql_query('SELECT * FROM final_project.home_credit_default_risk_application_train_clean', cursor)


# extract test data
application_test_clean = pd.read_sql_query('SELECT * FROM final_project.home_credit_default_risk_application_test_clean', cursor)



## Preprocessing


# Splitting data
X_train = application_train_clean.drop(columns=['TARGET'],axis=1)
y_train = application_train_clean['TARGET']
X_test = application_test_clean


# Numerical Columns
num=X_train.select_dtypes(exclude='object').columns


# Categorical Columns
cat=X_train.select_dtypes(include='object').columns



## Logistic Regression Modelling


# import libraries
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer

from sklearn.linear_model import LogisticRegression
from imblearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split

from sklearn.metrics import precision_recall_fscore_support


# Transformer and Modelling Steps
cat_transformer = Pipeline([
    ("c_i", SimpleImputer(strategy="most_frequent")),
    ("e", OneHotEncoder(handle_unknown = "ignore"))
])

num_transformer = Pipeline([
    ("n_i", SimpleImputer(strategy="median"))
])

transformers = [
    ("n_t", num_transformer, num),
    ("c_t", cat_transformer, cat)
]

model = Pipeline([
    ("pre", ColumnTransformer(transformers=transformers)),
    ("model", LogisticRegression(max_iter=1000, random_state=1))
])


# Fitting model
model.fit(X_train, y_train)


# Predict
y_test = model.predict(X_test)


# Prediction Probability
y_prob = model.predict_proba(X_test)




# ## Upload ML Result to Postgres
# get id
ml_result = X_test[['SK_ID_CURR']]

# add ml result
ml_result['prediction_target'] = y_test.tolist()
ml_result['probability'] = y_prob.tolist()


# upload ml result to postgres
ml_result.to_sql('home_credit_default_risk_application_ml_result', index=False, conn=cursor, schema='final_project', if_exists='replace')