import pandas as pd

# Load the CSV file into a DataFrame, treating 'billing_code' as a string
df = pd.read_csv('clean_result3.csv', dtype={'billing_code': str})

# Ensure that 'negotiated_rate' is numeric
df['negotiated_rate'] = pd.to_numeric(df['negotiated_rate'], errors='coerce')

# Filter out rows where 'billing_code' is '33963'
df_filtered = df[df['billing_code'] != '33963']

# Group by 'billing_code' and 'billing_class', then calculate the median, count, and standard deviation
statistics = df_filtered.groupby(['billing_code', 'billing_class']).agg(
    median_negotiated_rate=('negotiated_rate', 'median'),
    count=('negotiated_rate', 'size'),
    std_deviation=('negotiated_rate', 'std')
).reset_index()

# Round the standard deviation to two decimal places
statistics['std_deviation'] = statistics['std_deviation'].round(2)

# Save the results to a CSV file
statistics.to_csv('statistics.csv', index=False)

# Print the results
print(statistics)
