import pandas
import sys
df = pandas.read_csv(sys.stdin)
print(df.iloc[:,3].value_counts())
