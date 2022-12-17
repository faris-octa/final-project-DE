# Import required libraries
import pymongo
import pandas as pd
import psycopg2

# Buat koneksi ke MongoDB Atlas
uri = "mongodb+srv://farisocta:farizuniX04@atlas-cluster.rucxbhe.mongodb.net/?retryWrites=true&w=majority"
client = pymongo.MongoClient(uri)
db = client["sample_training"]
dataframe = {}

# zips collection
collection = db['zips']

# flat field loc menjadi longitude, latitude
try:
  zips_cursor = collection.aggregate([
    {
        "$project": {
          "city": 1,
          "zip": 1,
          "latitude": "$loc.x",
          "longitude": "$loc.y",
          "pop": 1,
          "state": 1
        }
    }
    ])
except:
  print("gagal membaca collection zips")

zips_df = pd.DataFrame(zips_cursor)
zips_df = zips_df.drop(columns=['_id'])
dataframe['zips'] = zips_df



# companies collection
collection = db['companies']

# Iterasi untuk mengambil semua kolom yang tidak bersarang dan mengambil nilai pada object pertama di field 'offices'
array_collection = []
try:
    
    for document in collection.find():
        company_dict = {} 
        for key, value in document.items():
            if key == '_id':
                #company_dict["id"] = str(value) --uncomment jika ingin di include
                continue

            elif key == 'offices':
                if len(value) > 0:
                    for subkey, subvalue in value[0].items():
                        company_dict["offices_" + subkey] = subvalue
                else:
                    continue

            elif type(value) != dict or type(value) != list:
                company_dict[key] = value
    array_collection.append(company_dict)
except:
  print("gagal membaca collection companies")

companies_df = pd.DataFrame(array_collection)
dataframe['companies'] = companies_df


# import to postgres DWH

# Mengkoneksikan ke Postgres container
try:
    conn = psycopg2.connect(
        host="postgres-ds9",
        database="postgres",
        user="postgres",
        password="password"
    )
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to Postgres", error)

# Membaca dataframe zips dan menyimpannya sebagai tabel zips
for key, value in dataframe.items():
    value.to_sql(name=key, con=conn, if_exist='replace', index=False)

# Menutup koneksi ke Postgres container
conn.close()
