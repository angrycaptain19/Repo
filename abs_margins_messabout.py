# -*- coding: utf-8 -*-
"""
Created on Mon Sep  6 10:45:51 2021

@author: hac
"""




####  abs margin


def absolute_margins():

    
    import pandas as pd
    import datetime
    import pytz
    from datetime import datetime, timezone
    import time
    
    prompt_db_raw = pd.read_parquet(r"X:\Power\UK\HAC\Tableau\master_df.parquet")
    melted_db_raw = pd.read_parquet(r"X:\Power\UK\HAC\Tableau\av_hh.parquet")
    
    prompt_db_raw['index'] = prompt_db_raw['index'].dt.tz_localize(None)   
    
    melted_db = melted_db_raw[['Date_stamp','Date','SP','instance','Fuel','Derated_Av']]

    unmelted_df = melted_db.pivot_table(index=['Date_stamp','Date','SP','instance'], columns='Fuel', values='Derated_Av').reset_index()
 
    residual_db = prompt_db_raw.loc[prompt_db_raw["product"] == "rdl uk"]
    
    del residual_db['product']
    residual_db = residual_db.rename(columns={'index':'Date_stamp'})
    
    unmelted_df['Date_stamp'] = pd.to_datetime(unmelted_df['Date_stamp'])
    
    residual_db['instance'] = residual_db['instance'].apply(lambda x: x.strftime('%Y%m%d'))
    
    db_test = residual_db.merge(unmelted_df, on=['Date_stamp','instance'], how='inner')
    
    Hours = db_test['Date_stamp'].dropna().dt.hour.astype(int)
    
    Hours = Hours.rename("Hour")
      
    db_test['Block'] = Hour2Block(Hours, "BlockGroup")
    
    db_test['Split'] = Hour2Block(Hours, "DayGroup")
    
    db_test['HalfBlocks'] = Hour2Block(Hours, "1/2Block")
    
    db_test.to_csv(r"X:\Power\UK\HAC\Tableau\db_test.csv")
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    db_test['Total_Av'] = (db_test['Battery'] + db_test['Biomass'] + db_test['CCGT_MR'] + db_test['Coal'] + db_test['Gas CCGT'] + db_test['Gas OCGT'] + 3750 + db_test['Nuke'] + db_test['Pumped Storage'] + db_test['Reservoir hydro'] )
    
    db_test['Abs_Margin'] = db_test['Total_Av'] - db_test['value']
    
    db_test['Scarce_Units'] = db_test['Battery'] + db_test['Gas OCGT'] + db_test['Pumped Storage']
    
    












        

        
        





















