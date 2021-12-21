"""
Data Pre-processing Module

This module performs the cleaning of the collected datasets
and then stores the cleaned data in SQL database tables.

We use Python Pandas package to import the data and then transform
it to the desired format. MySQL Connector Python package is used to
connect to a MySQL database server and store the cleaned data 
"""

author = "Tanuja Seervi, Bikiran Choudhury"


import pandas as pd
from os import path

import mysql.connector
from getpass import getpass


def fix_column_name(df):
    
    """
    This function checks and removes any whitespace
    before and after the column name. And replaces
    spaces between words with an underscore.
    
    Input:
        df: Pandas DataFrame
    
    Return:
        None
    
    """
    
    df.columns = [c.strip().replace(" ", "_") for c in df.columns]
    
    return None


def find_total_missing_val_and_loc(df):
    
    """
    This function prints the total number of missing values
    in the given DataFrame for each column and returns the
    records with missing value. Otherwise, it returns "None".
    
    Input:
        df: Pandas DataFrame
    
    Return:
        Pandas DataFrame
    
    """

    missing_val = df.isnull().sum()
    
    if sum(missing_val):
        return df.loc[df.isnull().T.any()]

    else:
        return None


def normalize_country_names(df, country_col):
    
    """
    Return the Unicode normal form of the strings of the
    Pandas Series with the name: country_col.
       
    Input:
        df: Pandas DataFrame
        country_col: string
    
    Returns
        df_tmp: Pandas Series
    """
    
    # Get the column which is having country name from the table
    df_tmp = df.copy() 
    country_names = df_tmp[country_col]

    # Replace `_` with whitespace
    country_names = country_names.str.replace("_", " ")

    # Remove parenthesis and any content and whitespace around it
    country_names = country_names.str.replace("\s*\(.*\)\s*", '',regex=True)
    
    # normalise the string values to remove symbols and diacritics
    norm_name = country_names.str.normalize('NFKD')
    norm_name = norm_name.str.encode('ascii',errors='ignore').str.decode('utf-8')

    df_tmp[country_col] = country_names
    
    return df_tmp


def filter_unmatched_index(df_primary, df_ancillary, col_primary, col_ancillary):
    """
    This function returns the non-matching entries on columns
    given with indexes in columns 'primary' and 'ancillary'
    
    This function also saves each DF's index as 'index_primary'
    and 'index_ancillary', and return the records which are not
    matching between the two columns.
    """
    df_p = df_primary[[col_primary]]
    df_p.index.name = 'index_primary'
    df_p.reset_index(inplace=True)
    
    df_a = df_ancillary[[col_ancillary]]
    df_a.index.name = 'index_ancillary'
    df_a.reset_index(inplace=True)
    
    df_outer = pd.merge(df_p, df_a, left_on=col_primary, right_on=col_ancillary, how="outer")
    index_nulls = find_total_missing_val_and_loc(df_outer)
    
    return index_nulls


def get_close_matches(name, list_compare, pad=True):
    
    """
    This function returns the best matching entry
    from 'list_compare' to 'name'
    
    Input:
        name: string
        list_compare: [string]
        
    """
    from difflib import get_close_matches
    match_list = get_close_matches(name, list_compare, n=1, cutoff=0.5)
    
    if len(match_list):
        return match_list[0]
    else:
        return name if pad else None


def find_divergence(df_pop, df, col_name):
    """
    This function adds "CnT-pad" and "CnT-noPad" and
    returns the records with non-matching country names
    of df with df_population.
    
    Input:
        df: Pandas DataFrame
        col_name: string
    
    Return:
        Pandas DataFrame

    """
    df_2 = df.copy()

    # merge tables
    df_merge_unmatch = filter_unmatched_index(df_pop, df_2,"name", col_name)

    # Define the country name in "df_2" as the primary series
    # Then modify with the values from "Population-2020-21" table
    primary = df_merge_unmatch[col_name].dropna()
    options = df_merge_unmatch["name"].dropna()

    # Add column 'CnT-pad' to "df_merge_unmatch" with either the best match from options (if any)
    # or the primary/self (if there is no good match)
    df_merge_unmatch["CnT-pad"] = primary.apply(lambda country: get_close_matches(country, options))

    # Add column 'CnT-noPad' to "df_merge_unmatch" which has None when there is no good match
    df_merge_unmatch["CnT-noPad"] = primary.apply(lambda country: get_close_matches(country, options, pad=False))

    return df_merge_unmatch[["index_primary", "name", "index_ancillary", col_name,"CnT-pad","CnT-noPad"]]



def main():
    """
    This functions imports all the datasets into pandas dataframe,
    cleans the data and stores in a SQL database.
    """

    # full path where data files are stored
    dir_path = input("Enter the directory where data files are stored: " )

    # Import all the datasets

    df_tokyo = pd.read_csv(path.join(dir_path,"Tokyo_Medals_2020.csv"), header=0)
    df_rio = pd.read_csv(path.join(dir_path,"Rio_Medals_2016.csv"), header=0)
    df_london = pd.read_csv(path.join(dir_path,"London_Medals_2012.csv"), header=0)
    df_population = pd.read_csv(path.join(dir_path,"Population_2020-21.csv"), header= 0)
    df_covid_vac = pd.read_csv(path.join(dir_path,"Covid_Vaccination_Data.csv"), header= 0)
    df_gdp = pd.read_csv(path.join(dir_path,"GDP_Actual_Value.csv"), header= 0, encoding="ISO-8859-1")


    # Part-I of Data Pre-processing

    # Basic cleaning of df_tokyo
    # Drop 'Rank By Total' column
    df_tokyo = df_tokyo.drop(columns="Rank By Total", axis=1)

    # Fix column names of the dataframe
    fix_column_name(df_tokyo)

    # Clean the country names
    df_tokyo = normalize_country_names(df_tokyo, "Country")


    # Basic cleaning of df_rio
    # Add column "Total"
    df_rio["Total"] = df_rio.iloc[:,1:3].sum(axis=1)

    # Fix column names of the dataframe
    fix_column_name(df_rio)

    # Clean the country names
    df_rio = normalize_country_names(df_rio, "Country")


    # Basic cleaning of df_london
    # Fix column names of the dataframe
    fix_column_name(df_london)

    # Clean the country names
    df_london = normalize_country_names(df_london, "Country")


    # Basic cleaning of df_population
    # Drop non-essential columns
    df_population = df_population[["name","pop2021","pop2020"]]

    # Fix column names of the dataframe
    fix_column_name(df_population)

    # Clean the country names
    df_population = normalize_country_names(df_population, "name")

    # Rectify population values
    df_population['pop2021'] = df_population['pop2021'].apply(lambda x : x * 1000)
    df_population['pop2020'] = df_population['pop2020'].apply(lambda x : x * 1000)


    # Basic cleaning of df_covid_vac
    # Drop unncessary columns
    df_covid_vac = df_covid_vac[["location", "date", "total_cases", "new_cases", \
            "total_deaths", "new_deaths", "people_vaccinated", "people_fully_vaccinated"]]

    # Remove non-country entries
    to_remove = ["Africa", "Asia", "Europe", "European Union", "High income", \
            "International", "Low income", "Lower middle income", \
            "North America", "Oceania","South America", "Upper middle income","World"]

    for name in to_remove:
        df_covid_vac.drop(df_covid_vac[df_covid_vac['location'] == name].index, inplace = True)


    # Fix column names of the dataframe
    fix_column_name(df_covid_vac)

    # Replace NaN values with 0
    df_covid_vac.fillna(value=0, inplace=True)

    # Clean the country names
    df_covid_vac = normalize_country_names(df_covid_vac, "location")


    # Basic cleaning of df_gdp
    # Rename country name column
    df_gdp.rename(columns={"GDP, current prices (Billions of U.S. dollars)": "Country"}, inplace=True)

    # Drop non-essential columns
    df_gdp = df_gdp[["Country","2012","2013","2014","2015","2016","2017","2018","2019","2020","2021"]]

    # Fix column names of the dataframe
    fix_column_name(df_gdp)

    # Remove entries with no country name
    df_gdp.drop([0,229,230], axis=0, inplace=True)

    # Remove entries of different regions other than country
    df_gdp.drop(df_gdp.index[197:228], axis=0, inplace=True)

    # Replace 'no data' entries with 0
    df_gdp.replace(['no data'], [0], inplace=True)

    # Clean the country names
    df_gdp = normalize_country_names(df_gdp, "Country")


    # Part-II of Data Pre-processing

    # for df_tokyo
    df_tmp = find_divergence(df_population, df_tokyo, df_tokyo.columns[0])

    # Copy the 'CnT-pad' column and then fix wrong values manually
    df_tmp["pop_fixed"] = df_tmp["CnT-pad"].copy()

    df_tmp.loc[232, 'pop_fixed'] = "Ivory Coast"
    df_tmp.loc[234, 'pop_fixed'] = "United Kingdom"
    df_tmp.loc[236, 'pop_fixed'] = "Iran"
    df_tmp.loc[238, 'pop_fixed'] = "China"
    df_tmp.loc[239, 'pop_fixed'] = "South Korea"
    df_tmp.loc[240, 'pop_fixed'] = "Moldova"
    df_tmp.loc[241, 'pop_fixed'] = "Russia"
    df_tmp.loc[242, 'pop_fixed'] = "Syria"
    df_tmp.loc[233, 'pop_fixed'] = "Taiwan"

    # Now put the obtained values back into the df_2 data frame with the correct indexes
    index_tmp = df_tmp['index_ancillary'].dropna().astype(int)
    names_tmp = df_tmp['pop_fixed'].dropna().values
    df_tokyo.loc[index_tmp, df_tokyo.columns[0]] = names_tmp

    del df_tmp


    # for df_rio
    df_tmp = find_divergence(df_population, df_rio, df_rio.columns[0])

    # Copy the 'CnT-pad' column and then fix wrong values manually
    df_tmp["pop_fixed"] = df_tmp["CnT-pad"].copy()

    df_tmp.loc[232, 'pop_fixed'] = "Ivory Coast"

    # Now put the obtained values back into the df_2 data frame with the correct indexes
    index_tmp = df_tmp['index_ancillary'].dropna().astype(int)
    names_tmp = df_tmp['pop_fixed'].dropna().values
    df_rio.loc[index_tmp, df_rio.columns[0]] = names_tmp

    del df_tmp



    # for df_london
    df_tmp = find_divergence(df_population, df_london, df_london.columns[0])

    # Copy the 'CnT-pad' column and then fix wrong values manually
    df_tmp["pop_fixed"] = df_tmp["CnT-pad"].copy()

    df_tmp.loc[232, 'pop_fixed'] = "Taiwan"
    df_tmp.loc[233, 'pop_fixed'] = "North Korea"
    df_tmp.loc[234, 'pop_fixed'] = "United Kingdom"
    df_tmp.loc[236, 'pop_fixed'] = "Iran"
    df_tmp.loc[237, 'pop_fixed'] = "China"
    df_tmp.loc[238, 'pop_fixed'] = "South Korea"

    # Now put the obtained values back into the df_2 data frame with the correct indexes
    index_tmp = df_tmp['index_ancillary'].dropna().astype(int)
    names_tmp = df_tmp['pop_fixed'].dropna().values
    df_london.loc[index_tmp, df_london.columns[0]] = names_tmp

    del df_tmp


    # for df_covid_vac
    df = pd.DataFrame(df_covid_vac.location.unique(), columns=["Country_Name"])
    df_tmp = find_divergence(df_population, df, df.columns[0])

    # Copy the 'CnT-pad' column and then fix wrong values manually
    df_tmp["pop_fixed"] = df_tmp["CnT-pad"].copy()

    df_tmp.loc[233, 'pop_fixed'] = "Republic of the Congo"
    df_tmp.loc[234, 'pop_fixed'] = "Ivory Coast"
    df_tmp.loc[236, 'pop_fixed'] = "DR Congo"
    df_tmp.loc[242, 'pop_fixed'] = "Northern Cyprus"
    df_tmp.loc[244, 'pop_fixed'] = "Saint Helena"


    # Now put the obtained values back into the df_2 data frame with the correct indexes
    index_tmp = df_tmp['index_ancillary'].dropna().astype(int)
    names_tmp = df_tmp['pop_fixed'].dropna().values
    df.loc[index_tmp, df.columns[0]] = names_tmp


    old_val = ["Congo", "Cote d'Ivoire", "Democratic Republic of Congo"]
    new_val = ["Republic of the Congo", "Ivory Coast", "DR Congo"]

    df_covid_vac['location'] =  df_covid_vac['location'].replace(old_val,new_val)


    del df, df_tmp



    # for df_gdp
    df_tmp = find_divergence(df_population, df_gdp, df_gdp.columns[0])

    # Copy the 'CnT-pad' column and then fix wrong values manually
    df_tmp["pop_fixed"] = df_tmp["CnT-pad"].copy()

    df_tmp.loc[235, 'pop_fixed'] = 'China'
    df_tmp.loc[236, 'pop_fixed'] = "DR Congo"
    df_tmp.loc[238, 'pop_fixed'] = "Ivory Coast"
    df_tmp.loc[241, 'pop_fixed'] = "South Korea"
    df_tmp.loc[243, 'pop_fixed'] = 'Kyrgyzstan'
    df_tmp.loc[244, 'pop_fixed'] = 'Laos'
    df_tmp.loc[252, 'pop_fixed'] = "Taiwan"

    df_tmp.loc[253, 'pop_fixed'] = "West Bank and Gaza"
    df_tmp.loc[254, 'pop_fixed'] = "Africa"

    # Now put the obtained values back into the df_2 table with the correct indexes
    index_tmp = df_tmp['index_ancillary'].dropna().astype(int)
    names_tmp = df_tmp['pop_fixed'].dropna().values
    df_gdp.loc[index_tmp, df_gdp.columns[0]] = names_tmp

    del df_tmp


    # Store the datasets in a SQL database server

    # Connect and login into MySQL database server
    connection = mysql.connector.connect(user=input("Enter username: "), password=getpass("Enter password: "))
    cursor = connection.cursor()

    # Create and select database
    create_db_query = "CREATE DATABASE IF NOT EXISTS olympic"
    cursor.execute(create_db_query)

    select_db_query = "use olympic"
    cursor.execute(select_db_query)

    # Create Tables
    tables_def = {
        "tokyo_olympic_2020" : """
                                CREATE TABLE IF NOT EXISTS tokyo_olympic_2020 (
                                    country_name VARCHAR(60) NOT NULL,
                                    gold_medals INT NOT NULL,
                                    silver_medals INT NOT NULL,
                                    bronze_medals INT NOT NULL,
                                    total_medals INT NOT NULL,

                                    CONSTRAINT uniq_constraint UNIQUE(country_name)
                                )
                            """,


        "rio_olympic_2016" : """
                                CREATE TABLE IF NOT EXISTS rio_olympic_2016 (
                                    country_name VARCHAR(60) NOT NULL,
                                    gold_medals INT NOT NULL,
                                    silver_medals INT NOT NULL,
                                    bronze_medals INT NOT NULL,
                                    total_medals INT NOT NULL,

                                    CONSTRAINT uniq_constraint UNIQUE(country_name)
                                )
                        """,

        "london_olympic_2012" : """
                                CREATE TABLE IF NOT EXISTS london_olympic_2012 (
                                    country_name VARCHAR(60) NOT NULL,
                                    gold_medals INT NOT NULL,
                                    silver_medals INT NOT NULL,
                                    bronze_medals INT NOT NULL,
                                    total_medals INT NOT NULL,
                                    
                                    CONSTRAINT uniq_constraint UNIQUE(country_name)
                                )
                                """,

        "population" : """
                        CREATE TABLE IF NOT EXISTS population (
                            country_name VARCHAR(60) NOT NULL,
                            pop_2020 INT UNSIGNED NOT NULL,
                            pop_2021 INT UNSIGNED NOT NULL,
                            
                            CONSTRAINT uniq_constraint UNIQUE(country_name)
                        )
                    """,

        "covid_and_vac" : """
                            CREATE TABLE IF NOT EXISTS covid_and_vac (
                                country_name VARCHAR(60) NOT NULL,
                                date_reported DATE NOT NULL,
                                cumulative_cases INT UNSIGNED NOT NULL,
                                new_cases INT NOT NULL,
                                cumulative_deaths INT UNSIGNED NOT NULL,
                                new_deaths INT NOT NULL,
                                people_vaccinated INT UNSIGNED NOT NULL,
                                people_fully_vaccinated INT UNSIGNED NOT NULL,
                                
                                
                                CONSTRAINT uniq_constraint UNIQUE(country_name, date_reported)
                            )
                        """,

        "gdp_value" : """
                        CREATE TABLE IF NOT EXISTS gdp_value (
                            country_name VARCHAR(60) NOT NULL,
                            gdp_2012 FLOAT NOT NULL,
                            gdp_2013 FLOAT NOT NULL,
                            gdp_2014 FLOAT NOT NULL,
                            gdp_2015 FLOAT NOT NULL,
                            gdp_2016 FLOAT NOT NULL,
                            gdp_2017 FLOAT NOT NULL,
                            gdp_2018 FLOAT NOT NULL,
                            gdp_2019 FLOAT NOT NULL,
                            gdp_2020 FLOAT NOT NULL,
                            gdp_2021 FLOAT NOT NULL,
                            
                            CONSTRAINT uniq_constraint UNIQUE(country_name)
                        )
                    """
        }

    # Create all the tables in the database
    for query in tables_def.values():
        cursor.execute(query)

    
    # Write data into the database
    for i, row in df_tokyo.iterrows():
        sql = "INSERT INTO tokyo_olympic_2020 (country_name, gold_medals, silver_medals, \
            bronze_medals, total_medals) VALUES (%s,%s,%s,%s,%s)"
        
        cursor.execute(sql,tuple(row))
        
        connection.commit()
    
    for i, row in df_rio.iterrows():
        sql = "INSERT INTO rio_olympic_2016 (country_name, gold_medals, silver_medals, \
            bronze_medals, total_medals) VALUES (%s,%s,%s,%s,%s)"

        cursor.execute(sql,tuple(row))

        connection.commit()

    for i, row in df_london.iterrows():
        sql = "INSERT INTO london_olympic_2012 (country_name, gold_medals, silver_medals, \
            bronze_medals, total_medals) VALUES (%s,%s,%s,%s,%s)"

        cursor.execute(sql,tuple(row))

        connection.commit()
    
    for i, row in df_population.iterrows():
        sql = "INSERT INTO population (country_name, pop_2020, pop_2021) VALUES (%s,%s,%s)"

        cursor.execute(sql,tuple(row))

        connection.commit()

    for i, row in df_covid_vac.iterrows():
        sql = "INSERT INTO covid_and_vac (country_name, date_reported, cumulative_cases, new_cases, \
            cumulative_deaths, new_deaths, people_vaccinated, people_fully_vaccinated) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"

        cursor.execute(sql,tuple(row))

        connection.commit()
    
    for i, row in df_gdp.iterrows():
        sql = "INSERT INTO gdp_value (country_name, gdp_2012, gdp_2013, gdp_2014, \
            gdp_2015, gdp_2016, gdp_2017, gdp_2018, gdp_2019, gdp_2020, gdp_2021) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        cursor.execute(sql,tuple(row))

        connection.commit()
    

    # Close the database connections
    cursor.close()
    connection.close()



if __name__ == "__main__":
    main()