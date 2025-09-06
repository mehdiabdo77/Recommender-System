import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import warnings
import dask.dataframe as dd
# غیرفعال کردن تمام هشدارها
warnings.filterwarnings("ignore")
from tqdm import tqdm
from sqlalchemy import create_engine
from functools import partial
import logging

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
# فعال کردن tqdm برای pandas
tqdm.pandas()
R = 6371.0

username = 'mahdi'
password = '27000147'  
host = '192.168.240.20'
port = '3306'
database = 'ghazal'

excluded_columns = ['lat', 'long']

def haversine_distance(lat1, lon1, lat2, lon2):
    """
         محاسبه مسافت خطی   
    """
    
    lat1_rad = np.radians(float(lat1))  
    lat2_rad = np.radians(float(lat2)) 
    lon1_rad = np.radians(float(lon1))  
    lon2_rad = np.radians(float(lon2))  

    
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad
    haversine_delta_lat = np.sin(delta_lat / 2) ** 2
    haversine_delta_lon = np.sin(delta_lon / 2) ** 2

   
    a = haversine_delta_lat + \
        np.cos(lat1_rad) * np.cos(lat2_rad) * haversine_delta_lon
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    
    distance = R * c
    return distance


def op(weights, customer_1_scores, customer_2_scores, df_matrix, excluded_columns=None, distance_km = 0) :
    if excluded_columns is None:
        excluded_columns = []

    # حذف ستون‌های مستثنی‌شده
    if excluded_columns:
        mask = ~np.isin(df_matrix.columns, excluded_columns)
        customer_1_scores = customer_1_scores[mask]
        customer_2_scores = customer_2_scores[mask]
        weights = weights[mask]
        
    if distance_km is not None and distance_km > 3.5:
        return 0  # اگر فاصله بیشتر از 3 کیلومتر باشد، شباهت صفر است

    # محاسبه مخرج برای جلوگیری از تقسیم صفر
    denominator = np.sum(weights * np.maximum(customer_1_scores, customer_2_scores))
    if denominator == 0:
        return 0  

    # محاسبه شباهت وزن‌دار
    weighted_similarity = np.sum(weights * np.minimum(customer_1_scores, customer_2_scores)) / denominator
    return weighted_similarity




def recomender(custumercode, df ,cv = 9 , n = 14):
    """
    این تابع برای پیشنهاد کالا به مشتریان بر اساس شباهت خریدهای قبلی آن‌ها طراحی شده است.

    پارامترها:
        custumercode (int or str): کد مشتری که می‌خواهیم برای او پیشنهاد ارائه دهیم.
        df (pandas.DataFrame): جدول داده‌های خرید مشتریان که شامل ستون‌های کد مشتری، موقعیت جغرافیایی (lat, long) و مقادیر خرید هر کالا است.
        cv (int, پیش‌فرض=9): حداکثر تعداد فاکتور برای هر کالا (سقف تعداد خرید هر کالا برای هر مشتری). اگر تعداد خرید بیشتر از این مقدار باشد، به این مقدار محدود می‌شود.
        n (int, پیش‌فرض=14): تعداد کالاهایی که به عنوان پیشنهاد به مشتری ارائه می‌شود (تعداد خروجی‌های پیشنهادی).

    خروجی:
        tuple:
            - نام کالاهای پیشنهادی (str)
            - رشته‌ای شامل نام کالاها و امتیاز هرکدام (str)

    توضیحات:
        این تابع ابتدا داده‌های مشتری مورد نظر را استخراج می‌کند، سپس با محاسبه شباهت بین مشتری هدف و سایر مشتریان (بر اساس خریدها و فاصله جغرافیایی)، کالاهایی را که مشتری تاکنون نخریده اما سایر مشتریان مشابه خریده‌اند، به او پیشنهاد می‌دهد. تعداد پیشنهادها با پارامتر n کنترل می‌شود و سقف تعداد خرید هر کالا با پارامتر cv محدود می‌گردد.
    """
    if len(str(custumercode))<=6:
        print(len(str(custumercode)))
    df = df.fillna(0)
    df = df[(df['lat'] != 0) & (df['long'] != 0)]
    # محدود کردن تعداد فاکتور
    cols_to_exclude = ['code', 'lat', 'long']
    df.loc[:, ~df.columns.isin(cols_to_exclude)] = df.loc[:, ~df.columns.isin(cols_to_exclude)].applymap(lambda x: cv if x >= cv else x)
    # دیتا بیس مشتری (فقط کد مشخص شده)
    code_df = df[df['code'] == custumercode]
    # موارد که مشتری خرید نداشته
    code_df_ziro = code_df.loc[:, (code_df == 0).any(axis=0)]
    # مواردی که مشتری خرید داشته
    code_df = code_df.loc[:, (code_df != 0).any(axis=0)]
    # برای محاسبه شباهت باید دیتا فریم کل مشتری ها با موارد که خرید کرده فیلتر بشه
    df_matrix_main = df[code_df.columns]
    # برای پیشنهاد سفارش نیاز است  مواردی که خرید نکرده در شباهت صرب بشه
    df_matrix_main_ziro = df[code_df_ziro.columns]


    # TODO باید اصلاح بشه
    df_matrix=df_matrix_main.drop(columns=['code'], errors='ignore')
    code_df.drop(columns=['code'], inplace=True)
    
    lat_ref = code_df['lat'].iloc[0]
    lon_ref = code_df['long'].iloc[0]
    df_matrix_main['distance_km']=df_matrix_main.apply(lambda row :  haversine_distance(lat_ref,lon_ref ,row['lat'], row['long']  ), axis=1  )
    df_matrix_main_ziro['distance_km']=df_matrix_main['distance_km']
    
    # columns_to_drop = ['lat', 'long']
    # df_matrix_main = df_matrix_main.drop(columns_to_drop, axis=1)  # حذف ستون‌های lat و long
    # df_matrix_main_ziro = df_matrix_main_ziro.drop(columns_to_drop, axis=1)

    y =pd.DataFrame()
    weights = np.array([1] * len(code_df.columns)) 
    y['similarity_percentage'] = df_matrix_main.apply(
        lambda row: op(weights, row[code_df.columns].values, code_df.iloc[0].values, df_matrix, distance_km= row['distance_km']) ,
        axis=1
        )
    
    if not df_matrix_main_ziro.empty:
        df_matrix_main_ziro = df_matrix_main_ziro.multiply(y["similarity_percentage"], axis=0)
        df_matrix_main_ziro['similarity_percentage'] = y['similarity_percentage']
        def conditional_sum_ratio(df, columns):
            df = df.fillna(0)
            result_dict = {}
            for i in columns:
                filtered_df = df[df[i] > 0]
                target_sum = filtered_df[i].sum()
                y_sum = filtered_df['similarity_percentage'].sum()
                if y_sum != 0:
                    result_dict[i] = target_sum / y_sum
            # مقادیر 0 رو باید حذف کنم
            result_dict = {k: v for k, v in result_dict.items() if v != 0}
            return dict(sorted(result_dict.items(), key=lambda item: item[1], reverse=True)[:n])
        
        top_columns = conditional_sum_ratio(
            df=df_matrix_main_ziro,
            columns=df_matrix_main_ziro.columns[
                ~df_matrix_main_ziro.columns.isin(["distance_km", "lat", 'code', 'long', 'similarity_percentage'])
            ]
        )
        
        # تبدیل دیکشنری به رشته برای ذخیره در DataFrame
        top_columns_str = " - ".join(f"{k}:{v:.2f}" for k, v in top_columns.items())
        top_columns_names = " - ".join(top_columns.keys())  # فقط نام کالاها
        
        return top_columns_names, top_columns_str  # بازگرداندن نام کالاها و رشته شامل کلیدها و مقادیر
    else:
        return "", ""

def process_code(code, df):
    try:
        return recomender(code, df)
    except Exception as e:
        logging.error(f"Error processing code: {code}")
        logging.error(f"Error details: {e}")
        raise  

df2= pd.read_excel("data2.xlsx")

if __name__ == "__main__":
    global_df = pd.read_excel("data.xlsx") 
    df2 = pd.read_excel("data2.xlsx")
    
    process_func = partial(process_code, df=global_df)

    with ProcessPoolExecutor() as executor:
        results = list(tqdm(executor.map(process_func, df2['code']), total=len(df2['code']), desc="Processing"))

    # جدا کردن نتایج به دو ستون
    df2['purchase_probability'], df2['suggested_scores'] = zip(*results)
    
    output_file_path = 'output_file.xlsx'
    df2.to_excel(output_file_path, index=False)
     
    connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
    print(connection_string) 
    engine = create_engine(connection_string)
    try:
        with engine.begin() as connection:
            print("Connected successfully")
    except Exception as e:
        print(f"Connection failed: {e}")
    
    df2.to_sql("recomender", con=engine, if_exists='append', index=False)

    