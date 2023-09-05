# Import necessary libraries
import pandas as pd
import numpy as np
import logging
import re

logger = logging.getLogger(__name__)

# Global config parameters
min_conv_thres = 10
cap_bid_thres = 12

# Read data
inventory_current = pd.read_excel('Inventory_Current_Onsite.xlsx', dtype={'Year': str}) # Change Year to string for easier merging
inventory_historical = pd.read_excel('Inventory_Historical.xlsx', dtype={'Year': str})
keyword_attributes = pd.read_excel('KW_Attributes.xlsx')
keyword_performance = pd.read_excel('KW_Performance_L120D.xlsx')
make_model_ars = pd.read_excel('Make_Model_ARS.xlsx')

# Global calculated parameter
ovr_cvr = sum(keyword_performance.Conversions) / sum(keyword_performance.Clicks) # Overall CVR

def extract_attributes(df):
    """
    Extract columns from ad group text.
    Params:
        df (pandas DataFrame): DataFrame containing Ad group data
    Returns:
        df: DataFrame with additional extracted columns
    """
    # Add validation check
    assert 'Ad group' in df.columns
    
    # Extract required attributes
    df['Market'] = df['Ad group'].str.extract(r'SRCH-I-(\w{3})', expand=False) # Extract market 
    df['Make'] = df['Ad group'].str.extract(r'-MK_(.*?)-', expand=False) # Extract make
    df['Model'] = df['Ad group'].str.extract(r'-MO_(.*?)-', expand=False) # Extract model
    df['Year'] = '20' + df['Ad group'].str.extract(r'-YR_(.*)', expand=False) # Extract year
    
    # Data validation
    try:
        assert df['Market'].notnull().all() 
        assert df['Make'].notnull().all()
        assert df['Model'].notnull().all()
        assert re.fullmatch('\d{4}', df['Year'].iloc[0])
    except AssertionError as e:
        logger.error("Validation failed: %s", e)
        return
    
    return df

def prepare_data(keyword_attributes, keyword_performance, make_model_ars, inventory_current, inventory_historical):
    """
    Prepare data via merging datasets and renaming columns to calculate bids.
    Params:
        keyword_attributes (pandas DataFrame): DataFrame containing keyword attributes
        keyword_performance (pandas DataFrame): DataFrame containing keyword performance
        make_model_ars (pandas DataFrame): DataFrame containing make and model-wise ARS
        inventory_current (pandas DataFrame): DataFrame containing current inventory
        inventory_historical (pandas DataFrame): DataFrame containing historical inventory
    Returns:
        df: DataFrame to calculate bids
    """
    # Merge Keyword data based on KW ID
    keyword_df = keyword_attributes.merge(keyword_performance, on='KW ID', how='left')

    # Merge ARS data with Keyword data based on Make Model
    keyword_ars_df = keyword_df.merge(make_model_ars.drop(columns='Make Model'), on=['Make', 'Model'], how='left')

    # Merge Current and Historical Inventory data based on Make Model Year
    keyword_ars_ci_df = keyword_ars_df.merge(inventory_current, on=['Make', 'Model', 'Year'], how='left')
    merged_df = keyword_ars_ci_df.merge(inventory_historical, on=['Make', 'Model', 'Year'], how='left')
    
    # Validate merge
    try:
        assert merged_df['ARS'].notnull().all()
        assert merged_df['Year'].str.len().max() == 4
    except AssertionError as e:
        logger.error("Validation failed: %s", e)
        return
    
    # Clean data
    merged_df['Year'] = merged_df['Year'].astype(int)
    
    # Rename columns for ease of use
    col_map = {
        'Campaign': 'cmpn',
        'Ad group': 'ag',
        'Keyword': 'kw',
        'KW ID': 'kw_id',
        'Match type': 'match_type',
        'Quality score': 'qs',
        'Est First Pos. Bid': 'efpb',
        'Est Top of Page Bid': 'etpb',
        'Impressions': 'impressions',
        'Clicks': 'clicks',
        'Cost': 'cost',
        'Conversions': 'conversions',
        'Market': 'mkt',
        'Make': 'mk',
        'Model': 'mo',
        'Year': 'yr',
        'ARS': 'ars',
        'CurrentOnsiteInventory': 'ci',
        'HistAvgInv': 'hi',
    }
    df = merged_df.rename(columns=col_map)
    
    return df

def calculate_metrics(df):
    """
    Calculate metrics needed to build logic for calculating bids.
    Params:
        df (pandas DataFrame): DataFrame to calculate bids
    Returns:
        df: DataFrame with required metrics at each desired level
    """
    # Aggregate (sum of) conversions at each desired level
    df['ag_conversions'] = df.groupby('ag')['conversions'].transform(sum) # Ad group level
    df['mmy_conversions'] = df.groupby(['mk', 'mo', 'yr'])['conversions'].transform(sum) # Mk/Mo/Yr level
    df['mm_conversions'] = df.groupby(['mk', 'mo'])['conversions'].transform(sum) # Mk/Mo level
    df['mkt_conversions'] = df.groupby(['mkt'])['conversions'].transform(sum) # Mkt level
    
    # Calculate CVR at each desired level
    df['kw_id_cvr'] = df.conversions / df.clicks # Keyword level
    df['ag_cvr'] = df.ag_conversions / df.groupby('ag')['clicks'].transform(sum) # Ad group level
    df['mmy_cvr'] = df.mmy_conversions / df.groupby(['mk', 'mo', 'yr'])['clicks'].transform(sum) # Mk/Mo/Yr level
    df['mm_cvr'] = df.mm_conversions / df.groupby(['mk', 'mo'])['clicks'].transform(sum) # Mk/Mo level
    df['mkt_cvr'] = df.mkt_conversions / df.groupby(['mkt'])['clicks'].transform(sum) # Mkt level

    return df

def calculate_initial_bid(row):
    """
    Calculate initial bid based on desired logic.
    Params:
        row (pandas Series): Row of data representing a keyword with its corresponding calculated metrics
    Returns:
        float: Calculated initial bid for the keyword
    """
    if row['conversions'] > min_conv_thres:
        return row['kw_id_cvr'] * row['ars']
    elif row['ag_conversions'] > min_conv_thres:
        return row['ag_cvr'] * row['ars']
    elif row['mmy_conversions'] > min_conv_thres:
        return row['mmy_cvr'] * row['ars']
    elif row['mm_conversions'] > min_conv_thres:
        return row['mm_cvr'] * row['ars']
    else:
        return row['efpb']

def adjust_bid_v1(row):
    """
    Adjust bid based on inventory, market CVR vs. overall CVR, and quality score.
    Params:
        row (pandas Series): Row of data representing a keyword with its corresponding calculated metrics and initial bid value
    Returns:
        float: Adjusted bid value for the keyword as per desired logic
    """
    # Adjust based on inventory difference between current and historical avg
    if row['ci'] < row['hi']:
        row['adjust_inv'] = (1 + ((row['ci'] / row['hi']) - 1) / 2)
        row['bid'] = row['bid'] * row['adjust_inv']
    
    # Adjust based on CVR difference between market level and overall level (only for bids calc. based on MMY/MM conversions)
    if (row['conversions'] <= 10) and (row['ag_conversions'] <= 10) and ((row['mmy_conversions'] > 10) or (row['mm_conversions'] > 10)):
        row['adjust_mkt'] = (1 + ((row['mkt_cvr'] / ovr_cvr) - 1) / 2)
        row['bid'] = row['bid'] * row['adjust_mkt']
    
    # Adjust based on quality score
    if row['qs'] > 7:
        row['bid'] = min(row['bid'], row['efpb'])
    elif row['qs'] > 5:
        row['bid'] = min(row['bid'], ((row['etpb'] * 0.5) + (row['efpb'] * 0.5)))
    else:
        row['bid'] = min(row['bid'], ((row['etpb'] * 0.9) + (row['efpb'] * 0.1)))
    
    # Cap all bids to $12 or lower
    if row['bid'] > cap_bid_thres:
        row['bid'] = cap_bid_thres
    
    return row['bid']

def min_exact_bid(df):
    """
    Calculate minimum exact match type bid for each Ad group after inventory, market CVR vs. overall CVR, and quality score adjustments.
    Params:
        df (pandas DataFrame): DataFrame after initial adjustments
    Returns:
        df: DataFrame with minimum exact match type bid for each Ad group after initial adjustments
    """
    # Get Minimum Exact Type Bid for each Ad Group  
    min_bids = df[df['match_type'] == 'Exact'].groupby(['ag'])['bid'].min()
    
    # Add new column with Minimum Exact Type Bid based on Ad Group
    df['min_exact_bid'] = df['ag'].map(min_bids)
    
    return df

def adjust_bid_v2(row):
    """
    Adjust bid based on max bid capping and minimum exact match type bid value for each Ad group applied to broad match type keywords.
    Params:
        row (pandas Series): Row of data representing a keyword with its corresponding calculated metrics and adjusted bid value
    Returns:
        float: Final bid value for the keyword as per desired logic
    """ 
    # Adjust based on Minimum Exact Type Bid for each Ad Group   
    if row['match_type'] == 'Broad':
        row['bid'] = min(row['bid'], row['min_exact_bid'])
    
    return row['bid']

def output_file(df):
    """
    Output final results to CSV.
    Params:
        df (pandas DataFrame): DataFrame with final bid value
    Returns:
        df: DataFrame in desired bid upload file format  
    """
    # Validate bid values before providing output
    try:
        assert df['bid'].between(0, cap_bid_thres).all()
    except AssertionError as e:
        logger.error("Validation failed: %s", e)
        return
    
    # Change required column names as per bid upload file format and write DataFrame to csv
    df = df.rename(columns={
        'kw_id': 'Keyword ID',
        'bid': 'Bid',
    })
    df[['Keyword ID', 'Bid']].to_csv('bid_upload_file.csv', index=False)

def main():
    """
    Main workflow.
    """    
    modified_keyword_attributes = extract_attributes(keyword_attributes)
    merged_df = prepare_data(modified_keyword_attributes, keyword_performance, make_model_ars, inventory_current, inventory_historical)
    final_df = calculate_metrics(merged_df)
    final_df['bid'] = final_df.apply(calculate_initial_bid, axis=1)
    final_df['bid'] = final_df.apply(adjust_bid_v1, axis=1)
    final_df = min_exact_bid(final_df)
    final_df['bid'] = final_df.apply(adjust_bid_v2, axis=1)
    output_file(final_df)
    
if __name__ == "__main__":
    main()