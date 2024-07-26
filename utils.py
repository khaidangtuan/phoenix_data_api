import pandas as pd

def curr_convert(price_in_hkd, curr_df, target_curr):
    if target_curr == 'hkd':
        return price_in_hkd
    else:
        hkd_vnd_exr = float(curr_df[curr_df['CurrencyCode'] == 'HKD']['Sell'].values[0].replace(',',''))
        price_in_vnd = price_in_hkd * hkd_vnd_exr
        if target_curr == 'vnd':
            return round(price_in_vnd,2)
        else:
            other_vnd_exr = float(curr_df[curr_df['CurrencyCode'] == target_curr.upper()]['Sell'].values[0].replace(',',''))
            return round(price_in_vnd/other_vnd_exr,2)