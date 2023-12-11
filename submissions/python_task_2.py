import pandas as pd


def calculate_distance_matrix(df)->pd.DataFrame:
    """
    Calculate a distance matrix based on the dataframe, df.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Distance matrix
    """
    # Check if the required columns are present
    # Create an empty distance matrix DataFrame
    tolls = sorted(set(df['id_start'].unique()) | set(df['id_end'].unique()))
    distance_matrix = pd.DataFrame(index=tolls, columns=tolls)
    distance_matrix = distance_matrix.fillna(0)

    # Populate the distance matrix with known distances
    for _, row in df.iterrows():
        start_toll, end_toll, distance = row['id_start'], row['id_end'], row['distance']
        distance_matrix.at[start_toll, end_toll] += distance
        distance_matrix.at[end_toll, start_toll] += distance  # Account for bidirectional distances

    return distance_matrix


def unroll_distance_matrix(distance_matrix):
    """
    Unroll a distance matrix to a DataFrame in the style of the initial dataset.

    Args:
        distance_matrix (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Unrolled DataFrame containing columns 'id_start', 'id_end', and 'distance'.
    """
    # Check if the required columns are present
    required_columns = ['id_start', 'id_end', 'distance']
    # Create a DataFrame to store unrolled distances
    unrolled_distances = pd.DataFrame(columns=required_columns)

    # Unroll the distance matrix
    for i, start_id in enumerate(distance_matrix.index):
        for j, end_id in enumerate(distance_matrix.columns):
            if i != j:  # Exclude same id_start to id_end combinations
                unrolled_distances = unrolled_distances.append({
                    'id_start': start_id,
                    'id_end': end_id,
                    'distance': distance_matrix.at[start_id, end_id]
                }, ignore_index=True)

    return unrolled_distances


def find_ids_within_ten_percentage_threshold(df, reference_id)->pd.DataFrame:
    """
    Find all IDs whose average distance lies within 10% of the average distance of the reference ID.

    Args:
        df (pandas.DataFrame)
        reference_id (int)

    Returns:
        pandas.DataFrame: DataFrame with IDs whose average distance is within the specified percentage threshold
                          of the reference ID's average distance.
    """
    # Check if the required columns are present
    # Filter rows where 'id_start' is equal to the reference value
    reference_rows = df[df['id_start'] == reference_id]

    # Calculate the average distance for the reference value
    reference_avg_distance = reference_rows['distance'].mean()

    # Calculate the lower and upper bounds for the 10% threshold
    lower_threshold = reference_avg_distance - (0.1 * reference_avg_distance)
    upper_threshold = reference_avg_distance + (0.1 * reference_avg_distance)

    # Filter 'id_start' values within the 10% threshold
    within_threshold_ids = df[
        (df['distance'] >= lower_threshold) &
        (df['distance'] <= upper_threshold)]['id_start'].unique()
    # Sort and return the list of 'id_start' values
    within_threshold_ids.sort()
    return within_threshold_ids


def calculate_toll_rate(df)->pd.DataFrame:
    """
    Calculate toll rates for each vehicle type based on the unrolled DataFrame.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Calculate toll rates based on vehicle types and add columns to the DataFrame
    df['moto'] = 0.8 * df['distance'] 
    df['car'] = 1.2 * df['distance']
    df['rv'] = 1.5 * df['distance']
    df['bus'] = 2.2 * df['distance']
    df['truck'] = 3.6 * df['distance']
    df.drop('distance', axis=1,inplace=True)
    
    return df
