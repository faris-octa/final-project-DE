
import pandas as pd 

from sqlalchemy import create_engine


from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer


# Koneksi ke database
conn = create_engine('postgresql://postgres:postgres-ds9@localhost:5432/postgres')

# Menulis query SQL untuk membaca tabel
query = "SELECT * FROM application_test"
home_credit_default_risk_application_test = pd.read_sql(query, conn)
query = "SELECT * FROM application_train"
home_credit_default_risk_application_train = pd.read_sql(query, conn)


# cek data train label
df_train['TARGET'].value_counts(normalize=True)

# DROP kolom yang memiliki nilai null > 60%
df_train.dropna(thresh=len(df) * 0.6, axis=1, inplace=True)

# preprocess data berdasarkan tipe data
num_cols = []
cat_cols = []
for i in range(len(df_train.dtypes)):
    if df_train.dtypes[i] == "object":
        cat_cols.append(df_train.dtypes[i])
    else:
        num_cols.append(df_train.dtypes[i])


# one-hot encoder for categorical columns
encoder = OneHotEncoder(sparse=False)

for col in cat_cols:
    df_encoded = encoder.fit_transform(df_train[[col]])
    df_encoded = pd.DataFrame(df_encoded, columns=[col + '_' + str(i) for i in range(df_encoded.shape[1])])
    df_train = pd.concat([df_train, df_encoded], axis=1)

df_train.drop(cat_cols, axis=1, inplace=True)

# median imputer for numerical columns
imp = SimpleImputer(strategy='median')

# Melakukan impute pada setiap kolom
for col in num_cols:
    df_train[col] = imp.fit_transform(df_train[[col]])


# store data to table postgres


# Menyimpan dataframe ke dalam tabel
df_train.to_sql('home_credit_default_risk_application_train', conn, if_exists='replace', index=False)
df_test.to_sql('home_credit_default_risk_application_test', conn, if_exists='replace', index=False)

conn.close()





"""

metode lain:
import psycopg2

with psycopg2.connect(
    host='localhost',
    port=5432,
    user='postgres',
    password='password',
    database='mydatabase'
) as conn:
    # Kode yang menggunakan koneksi database disini
    pass

"""