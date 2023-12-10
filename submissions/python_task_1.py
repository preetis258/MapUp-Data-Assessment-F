import pandas as pd
import numpy as np

def generate_car_matrix(df)->pd.DataFrame:
    """
    Creates a DataFrame for id combinations.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Matrix generated with 'car' values, 
                          where 'id_1' and 'id_2' are used as indices and columns respectively.
    """
    df_temporary = df.copy() # copying df into df_temporary for naming flexibility
    df_result = pd.DataFrame() # making df_result, an empty dataframe
    unique_ids = df_temporary.id_1.unique() # assigning unique values of id_1 from dataset-1 to unique_ids
    unique_ids.sort() # sorting the unique values of id_1 feature
    for i in unique_ids:
        temp_df = df_temporary[df_temporary.id_1==i][['car','id_2']].append({'car':0,'id_2':i},ignore_index=True).sort_values('id_2') # filtering out the dataframe and appending 0 whenever the values for id_1 and id_2 are same. Sorting based on id_2
        for idx, row in temp_df.iterrows(): 
            df_result.loc[i, row['id_2']] = row['car'] # filling up the values

    return df_result


def get_type_count(df)->dict:
    """
    Categorizes 'car' values into types and returns a dictionary of counts.

    Args:
        df (pandas.DataFrame)

    Returns:
        dict: A dictionary with car types as keys and their counts as values.
    """
    df['car_type'] = np.where(df['car']<=15,'low',
                              np.where((df['car']>15)&(df['car']<=25),'medium',
                                       np.where(df['car']>25,'high',None))) # categorizing car into car types based on the provided conditions
    grouped_data = {key: group for key, group in df.groupby('car_type').size().items()} # creating a dictionary containing counts of car_type
    sorted_grouped_data = dict(sorted(grouped_data.items(), key=lambda x: x[0])) # sorting the dictionary alphabetically using keys
    return sorted_grouped_data


def get_bus_indexes(df)->list:
    """
    Returns the indexes where the 'bus' values are greater than twice the mean.
    
    Args:
        df (pandas.DataFrame)
    
    Returns:
        list: List of indexes where 'bus' values exceed twice the mean.
    """
    mean_bus = df['bus'].mean() # calculating and creating a variable to store the mean value of bus
    bus_indexes = df[df['bus'] > 2 * mean_bus].index.tolist() # creating a list where 'bus' values exceed twice the mean
    bus_indexes.sort() # sorting the list
    return bus_indexes


def filter_routes(dataframe):
    """
    Filters and returns routes with average 'truck' values greater than 7.

    Args:
        dataframe (pandas.DataFrame)

    Returns:
        list: List of route names with average 'truck' values greater than 7.
    """
    # Group the DataFrame by the 'route' column and calculate the mean of the 'truck' column for each route.
    route_avg_truck = dataframe.groupby('route')['truck'].mean() 

    # Select routes where the average value of the 'truck' column is greater than 7.
    selected_routes = route_avg_truck[route_avg_truck > 7].index.tolist()

    # Sort the list of selected routes in ascending order.
    selected_routes.sort()

    # Return the sorted list of routes with average 'truck' values greater than 7.
    return selected_routes


def multiply_matrix(df)->pd.DataFrame:
    """
    Multiplies matrix values with custom conditions.

    Args:
        matrix (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Modified matrix with values multiplied based on custom conditions.
    """
    # Apply a lambda function to each element in the DataFrame, multiplying values by 0.75 if they are greater than 20,
    # and by 1.25 if they are 20 or less. This modifies the original values in the DataFrame.
    matrix = df.applymap(lambda x: x * 0.75 if x > 20 else x * 1.25) 

    # Round all values in the modified DataFrame to 1 decimal place.
    matrix = matrix.round(1)

    # Return the modified DataFrame with values adjusted based on the specified conditions.
    return matrix


def time_check(df)->pd.Series:
    """
    Use shared dataset-2 to verify the completeness of the data by checking whether the timestamps for each unique (`id`, `id_2`) pair cover a full 24-hour and 7 days period

    Args:
        df (pandas.DataFrame)

    Returns:
        pd.Series: return a boolean series
    """

    # Combine 'startDay' and 'startTime', 'endDay' and 'endTime' into datetime columns
    df['start_datetime'] = pd.to_datetime(df['startDay'] + ' ' + df['startTime'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    df['end_datetime'] = pd.to_datetime(df['endDay'] + ' ' + df['endTime'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    
    # Define the start and end time for a 24-hour period
    start_time = pd.to_datetime('00:00:00').time()
    end_time = pd.to_datetime('23:59:59').time()
    
    # Check if each (id, id_2) pair has correct timestamps
    completeness_check = df.groupby(['id', 'id_2']).apply(lambda group: 
        (
            (group['end_datetime'].max() - group['start_datetime'].min() >= pd.Timedelta(days=7)) &
            (group['start_datetime'].dt.time.min() == start_time) &
            (group['end_datetime'].dt.time.max() == end_time)
        )
    )
    
    return completeness_check
