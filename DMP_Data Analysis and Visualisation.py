"""
Data Analysis and Visualisation Module

This module performs the cleaning of the collected datasets
and then stores the cleaned data in SQL database tables.

We use Python Pandas package to import the data and then transform
it to the desired format. MySQL Connector Python package is used to
connect to a MySQL database server and store the cleaned data 
"""

author = "Tanuja Seervi, Bikiran Choudhury"


import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

from getpass import getpass


def get_all_country_performance(cursor, olympic_names, medals):
    """
    This function returns a dataframe containing country name
    and medals count for each of the given olympic names.

    Input:
        cursor: MySQLCursor
            cursor object to interact with the MySQL database server
        
        olympic_names: list
            List of Olympic games for which we want the medal counts
            ["tokyo_olympic_2020", "rio_olympic_2016", "london_olympic_2012"]
        
        medals: string
            The type of medal whose count we want
            "total_medals"/"gold_medals"/"silver_medals"/"bronze_medals"
    
    Return:
        df_final: Pandas DataFrame
    """
    
    # dummy empty dataframe
    df_final = pd.DataFrame()
    
    for olympic in olympic_names:
        select_medals_query = "SELECT country_name, {} FROM {}".format(medals,olympic)
        cursor.execute(select_medals_query)
        output_records = cursor.fetchall()
        df_tmp = pd.DataFrame(output_records, columns = ["Country_Name", olympic])
        
        if len(df_final):
            df_final = pd.merge(df_final, df_tmp,\
                            left_on="Country_Name",right_on="Country_Name", how="inner")
        else:
            df_final = df_tmp
            
    df_final = df_final.set_index("Country_Name")
    
    return df_final


def get_covid_death_vac_rate(cursor, country_name, year_span, covid_vac_col_name):
    """
    This function returns a dataframe which contains the per population rate
    of given a parameter: 'covid_vac_col_name' for reported_date
    
    The parameter 'covid_vac_col_name' can be any one of the below:
    'cumulative_cases', 'new_cases', 'cumulative_deaths', 'new_deaths',
    'people_vaccinated', 'people_fully_vaccinated'

    Input:
        cursor: MySQLCursor
            cursor object to interact with the MySQL database server
        
        country_name: list[strings]
            list of country names for which we want the results

        year_span: list[list]
            the time duration for which we want the values
        
        covid_vac_col_name: string
            the metric for which we want per population rate

    Return:
        Pandas DataFrame
    """
    
    # for population column
    pop_col_name = 'pop_'+year_span[0][0:4]

    # fetch population for given year
    select_population_query = "SELECT country_name, {} FROM population \
                                WHERE country_name = '{}'".format(pop_col_name,country_name)
    cursor.execute(select_population_query)
    
    population = cursor.fetchall()
    df_pop = pd.DataFrame(population, columns=["Country_Name", pop_col_name])
    
        
    # fetch desired metric for given year
    select_col_query = "SELECT country_name, date_reported, {} FROM covid_and_vac \
                WHERE country_name = '{}' and date(date_reported) BETWEEN '{}' AND '{}'".\
                format(covid_vac_col_name, country_name, year_span[0], year_span[1])
    cursor.execute(select_col_query)
    
    output_records = cursor.fetchall()
    df_cum_cases = pd.DataFrame(data=output_records, \
                        columns=["Country_Name", "Reported_Date", covid_vac_col_name])
        
    # Calculate per population rate of desired metric for given year
    df_tmp = pd.merge(df_cum_cases, df_pop[["Country_Name",pop_col_name]], \
                    left_on="Country_Name", right_on="Country_Name", how="inner")
        
    
    # Calculate the per population rate
    df_tmp["Rate"] = df_tmp[covid_vac_col_name]/df_tmp[pop_col_name]
    df_tmp = df_tmp.rename(columns={"Rate": country_name})
        
    
    return df_tmp[["Reported_Date",country_name]] 


def covid_death_vac_trend_plot(cursor, country_names, years, covid_vac_col_name):
    """
    This function shows(plots) the trends over the
    reported date for the given metric.

    Input:
        cursor: MySQLCursor
            cursor object to interact with the MySQL database server
        
        country_name:list[strings]
            list of country names for which we want the results

        year_span: list[list]
            the time duration for which we want the values

        covid_vac_col_name: string
            the metric for which we want the trend
    
    Return: None
    """
    
    # dummy empty dataframe
    df_final = pd.DataFrame()

    
    # Iterate over the country names and plot
    for name in country_names:
        
        df_years = pd.DataFrame()
        
        for year_span in years:
            df_tmp = get_covid_death_vac_rate(cursor, name, year_span, covid_vac_col_name)
            df_years = pd.concat([df_years,df_tmp], axis=0)
        
        if len(df_final):
            df_final = pd.merge(df_final,df_years,left_on="Reported_Date", \
                                right_on="Reported_Date", how="outer")
        else:
            df_final = df_years
    
    # set 'Reported_Date' as the index of the dataframe
    df_final = df_final.set_index("Reported_Date")

    
    if len(df_final):
        # plot the graph
        df_final.plot(logy=True)

        plt.title("{} per Population Trend".format(covid_vac_col_name.capitalize()))
        plt.legend(loc='upper left')
        plt.xlabel("Reported_Date")
        plt.ylabel("{} per Population".format(covid_vac_col_name.capitalize()))
        plt.grid(color="gray")
        plt.show()
    else:
        print("Data is not available for selected Country")
    
    return None


def get_country_gdp(cursor, country_names):
    """
    This fuction plots the GDP trend for given
    country_name(s) over a span of years

    Input:
        cursor: MySQLCursor
            cursor object to interact with the MySQL database server
        
        country_name: tuple(strings)
            name of the countries

    Return: None
    """
    
    # available values of gdp year
    years = ["gdp_2012","gdp_2013","gdp_2014","gdp_2015","gdp_2016","gdp_2017","gdp_2018",\
        "gdp_2019","gdp_2020","gdp_2021"]
    
    # get the gdp values for the mentioned country name
    if len(country_names) > 1:
        select_gdp_query = "SELECT * FROM gdp_value WHERE country_name IN {}".\
                            format(country_names)
        cursor.execute(select_gdp_query)
        gdp = cursor.fetchall()
    
    else:
        select_gdp_query = "SELECT * FROM gdp_value WHERE country_name = '{}'".format(country_names[0])
        cursor.execute(select_gdp_query)
        gdp = cursor.fetchall()
        
     
    data_list = []
    index_names =[]
    
    for g in gdp:
        data_list.append(g[1:11])
        index_names.append(g[0])

    df_final = pd.DataFrame(data=data_list, index=index_names, columns=years)

    if len(df_final):
        df_final.transpose().plot(marker='o')

        plt.title("GDP")
        plt.legend(loc='upper left')
        plt.xlabel("Year")
        plt.ylabel("GDP Value (in Billions of U.S. dollars)")
        plt.grid(color="gray")
        plt.show()
    else:
        print("Data is not available for selected Country")
    return None


def get_country_performance(cursor, country_name):
    """
    This function prints a dataframe with all the four medal counts
    ("Gold_Medals", "Silver_Medals", "Bronze_Medals", "Total_Medals")
    for all the three olympics games
    ("tokyo_olympic_2020", "rio_olympic_2016", "london_olympic_2012")

    for the given country name and also plot the results.
    
    Input:
        cursor: MySQLCursor
            cursor object to interact with the MySQL database server
        
        country_name: string
            name of the countries
        
    Return: None
    """
    
    olympic_games = ["tokyo_olympic_2020", "rio_olympic_2016", "london_olympic_2012"]
    data_list = []
    
    for olympic in olympic_games:
        select_all_medals_query = "SELECT gold_medals, silver_medals, bronze_medals, total_medals \
                                FROM {} WHERE country_name='{}'".format(olympic, country_name)

        cursor.execute(select_all_medals_query)
        output_records = cursor.fetchall()
        
        print(output_records)

        if len(output_records) == 0:
            data_list.append((0,0,0,0))
        else:
            data_list.append(output_records[0])

    df = pd.DataFrame(data=data_list,
                      index=olympic_games, 
                      columns=["Gold_Medals", "Silver_Medals", "Bronze_Medals", "Total_Medals"]
                     )
    print(df)

    # plot the bar graph
    df.plot.bar()
    
    plt.title("{}'s Performance in Olympic: Tokyo 2020, Rio 2016 & London 2012".format(country_name))
    plt.legend(loc='upper right')
    plt.xticks(rotation=0)
    plt.xlabel("Olympic Names")
    plt.ylabel("Medals Counts")
    plt.grid(color="gray")
    plt.show()
    
    return None


def get_all_trends(cursor, country_name):
    """
    This Function plots all the trends for a country

    Input:
        cursor: MySQLCursor
            cursor object to interact with the MySQL database server
        
        country_name: string
            name of the country

    Return: None
    """
    
    get_country_performance(cursor, country_name)
    
    year_list = [["2020-01-01", "2020-12-31"], ["2021-01-01", "2021-05-30"]]
    trend_list = ["cumulative_cases", "new_cases", "cumulative_deaths", "new_deaths",\
                  "people_vaccinated","people_fully_vaccinated"]
    
    for trend_name in trend_list:
        covid_death_vac_trend_plot(cursor, list([country_name]), year_list, trend_name)

    get_country_gdp(cursor, tuple([country_name]))
    
    return None


def main():
    """
    This functions queries required data from MySQL database and
    plots graphs for analysis.
    """
    # Connect and Login into SQL database
    connection = mysql.connector.connect(user=input("Enter username: "), password=getpass("Enter password: ")) 
    cursor = connection.cursor()

    # Select the database
    select_db_query = "use olympic"
    cursor.execute(select_db_query)

    # Analyse Performance of the countries in the three Olympic Games

    # Define the list of the olympics and medal types we want to compare
    olympics = ["tokyo_olympic_2020", "rio_olympic_2016", "london_olympic_2012"]
    medal_type = ["total_medals", "gold_medals", "silver_medals", "bronze_medals"]


    # Iterate over the country names and plot
    for medal in medal_type:
        
        # set plot figure parameters
        plt.rcParams['axes.facecolor'] = "White"
        
        # get the medal count of all the countries
        df_olympic_medals = get_all_country_performance(cursor, olympics, medal)
    
        pos = list(range(len(df_olympic_medals[olympics[0]])))
        width = 0.30

        # plot the bar graphs
        plt.bar(pos, df_olympic_medals[olympics[0]], width, \
                tick_label=df_olympic_medals.index,color='orange')
        
        plt.bar([p + width for p in pos], df_olympic_medals[olympics[1]], \
                width, color='darkturquoise')
        
        plt.bar([p + width*2 for p in pos], df_olympic_medals[olympics[2]], \
                width, color='purple',)

        # set plot axes parameters
        plt.title("{} Comparison".format(str(medal).capitalize()))
        plt.legend(olympics, loc='upper left')
        plt.xticks(rotation=90)
        plt.xlabel("Country Names")
        plt.ylabel("{}".format(str(medal).capitalize()))
        plt.grid(color="gray")
        plt.show()

    
    # Plot the trends for the available metrices for countries

    # Analysis for 5 countries for which we observed a drop in number of medals
    # define the start and end time for which we want to observe the values
    years = [["2020-01-01", "2020-12-31"], ["2021-01-01", "2021-06-30"]]

    # define the countries for which we want to observe the values
    countries = ["Argentina","Bahrain", "Colombia", "Taiwan", "Thailand","Venezuela"]

    # define the metrices for which we want to observe the plot
    trend_list = ["cumulative_cases", "new_cases", "cumulative_deaths", "new_deaths",\
                    "people_vaccinated","people_fully_vaccinated"]

    for trend_name in trend_list:
        covid_death_vac_trend_plot(cursor, countries, years, trend_name)
        
    get_country_gdp(cursor, tuple(countries))


    # Analysis for 5 countries for which we don't observe a drop in number of medals
    # define the start and end time for which we want to observe the values
    year_list = [["2020-01-01", "2020-12-31"], ["2021-01-01", "2021-06-30"]]

    # define the countries for which we want to observe the values
    country_name_list = ["Netherlands", "Poland", "Italy", "United Kingdom"]

    # define the metrices for which we want to observe the plot
    trend_list = ["cumulative_cases", "new_cases", "cumulative_deaths", "new_deaths",\
                    "people_vaccinated","people_fully_vaccinated"]

    for trend_name in trend_list:
        covid_death_vac_trend_plot(cursor, country_name_list, year_list, trend_name)
        
    get_country_gdp(cursor, tuple(country_name_list))


    # Analysis for any user selected country

    # Enter the country name
    country = input("Enter Country Name: ")
    get_all_trends(cursor, country)


    # Close the database connections
    cursor.close()
    connection.close()



if __name__ == "__main__":
    main()