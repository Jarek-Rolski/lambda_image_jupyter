import pandas as pd

def print_df():
    df = pd.DataFrame([[1,2],[3,4]], columns=['col1','col2'])
    print(df)

def handler(event, context):
    
    print(df)

    return 200

if __name__ == '__main__':
    handler(None, None)
