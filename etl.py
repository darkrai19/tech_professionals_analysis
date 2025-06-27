#%%
import pandas as pd
import re
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


# --- [E] EXTRACT ---
def extract_data(file_path):
    """
    Extracts data from a CSV file and returns a pandas DataFrame.
    """
    print(f"Extracting data from {file_path}...")
    
    df = pd.read_csv(file_path, usecols=['Age', 
        'YearsCode', 'YearsCodePro', 'EdLevel', 'LearnCode', 'LearnCodeOnline', 
        'LearnCodeCoursesCert', 'TechList', 'DevType', 
        'LanguageHaveWorkedWith', 'LanguageWantToWorkWith', 'DatabaseHaveWorkedWith', 
        'DatabaseWantToWorkWith', 'PlatformHaveWorkedWith', 'PlatformWantToWorkWith', 
        'ToolsTechHaveWorkedWith', 'ToolsTechWantToWorkWith']).dropna()
    
    return df

# --- [T] TRANSFORM ---
def parse_tech_list(tech_string):
    """
    Parses a string of technologies (separated by comma, semicolon, etc.)
    into a clean list of lower-case strings
    """
    if pd.isna(tech_string):
        return []
    
    technologies = re.split(r'[,;/]', str(tech_string))
    cleaned_list = [tech.strip().lower() for tech in technologies if tech.strip()]

    return cleaned_list

def transform_data(df):
    """Applies all transformation logic to the dataframe."""
    print("Transforming data...")

    have_worked_with = ['LanguageHaveWorkedWith', 'DatabaseHaveWorkedWith', 
                    'PlatformHaveWorkedWith', 'ToolsTechHaveWorkedWith']

    want_to_work_with = ['LanguageWantToWorkWith', 'DatabaseWantToWorkWith', 
                     'PlatformWantToWorkWith', 'ToolsTechWantToWorkWith']

    df[have_worked_with] = df[have_worked_with].map(parse_tech_list)
    df[want_to_work_with] = df[want_to_work_with].map(parse_tech_list)
    df['AllSkillsHaveWorkedWith'] = df[have_worked_with].sum(axis=1)
    df['AllSkillsWantToWorkWith'] = df[want_to_work_with].sum(axis=1)

    final_df = df['Age', 'YearsCode', 'YearsCodePro', 'EdLevel', 
        'LearnCode', 'LearnCodeOnline', 'LearnCodeCoursesCert', 'TechList', 
        'DevType', 'AllSkillsHaveWorkedWith', 'AllSkillsWantToWorkWith'].copy()

    return final_df

# --- [L] LOAD ---
def load_data(df, db_url):
    """Loads data into a PostgreSQL database."""
    print(f"Loading data into {db_name}.{table_name}...")

    engine = create_engine(f'postgresql://{db_name}')

# --- Main Execution ---
if __name__ == "__main__":
    RAW_DATA_PATH = 'survey_results_public.csv'

    raw_df = extract_data(RAW_DATA_PATH)
    transformed_df = transform_data(raw_df)

    print("\nPreview of transformed data:")
    print(transformed_df.head())
    


# %%


