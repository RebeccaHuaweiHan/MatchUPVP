
# read data as pandas DF from google sheet (not used)
def Read_from_google():
    import gspread

    from oauth2client.service_account import ServiceAccountCredentials

    # Authenticate
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "sodium-ceremony-464515-a4-8433bb999c93.json", scope)

    client = gspread.authorize(creds)

    # Open your sheet
    sheet = client.open("User and Organization Information").worksheet("VolunteerUsers")

    # # Get all records
    # data = sheet.get_all_records()
    # df = pd.DataFrame(data)
    #
    # print(df.head())

# Function to extract all quoted text
def extract_quoted(text):
    import re
    import pandas as pd
    if pd.isna(text):
        return None
    else:
        return re.findall(r'"(.*?)"', text)

def list_to_string(words):
    import pandas as pd
    if words is None:
        return None
    else:
        cleaned = [word.strip() for word in words]
        return ', '.join(cleaned)

def convert_pipe_to_comma(text):
    if text is None:
        return None
    else:
        return str(text).replace('|', ', ')

def replace_if_contains_target(text):
    if text is None:
        return None
    return text.replace("Children, Youth, Family", "Children Youth and Family")


# read data from xlsx file
def read_in_data():
    import pandas as pd
    Volunteer =pd.read_excel("UserOrganizationInformation.xlsx", sheet_name="VolunteerUsers")
    Vol_DF = Volunteer[['ID','User Email','volunteer-bio','professional-designations','volunteer-skills','volunteer-causes',
                        'volunteer-activities']]
    Vol_DF['volunteer-skills']=Vol_DF['volunteer-skills'].apply(extract_quoted)
    Vol_DF['volunteer-causes']=Vol_DF['volunteer-causes'].apply(extract_quoted)
    Vol_DF['volunteer-activities']=Vol_DF['volunteer-activities'].apply(extract_quoted)
    Vol_DF['volunteer-causes']=Vol_DF['volunteer-causes'].apply(list_to_string)
    Vol_DF['volunteer-skills']=Vol_DF['volunteer-skills'].apply(list_to_string)
    Vol_DF['volunteer-activities']=Vol_DF['volunteer-activities'].apply(list_to_string)
    Vol_DF['volunteer-causes']=Vol_DF['volunteer-causes'].apply(replace_if_contains_target)

    print(Vol_DF.head())
    print(Vol_DF.info())
    print(Vol_DF.iloc[0,5])
    print(type(Vol_DF.loc[0,'volunteer-causes']))
    Organization = pd.read_excel("UserOrganizationInformation.xlsx", sheet_name="Organizations")
    Org_DF = Organization[['ID', 'organization-email-address', 'Causes','Title','Permalink']]
    print(Org_DF.head())
    print(Org_DF.info())
    Org_DF['Causes']=Org_DF['Causes'].apply(convert_pipe_to_comma)
    Org_DF['Causes']=Org_DF['Causes'].apply(replace_if_contains_target)
    print(Org_DF.iloc[12,2])
    print(type(Org_DF.loc[0,'Causes']))
    return Vol_DF, Org_DF




def count_words_in_string(string_a, string_b):
    # This function checks how many words from a comma-separated string (string_a) appear in another string (string_b).
    if string_a is None or string_b is None:
        return None  # Return None if either input is missing

    words_a = [word.strip() for word in string_a.split(',')]
    string_b_lower = string_b.lower()
    return sum(1 for word in words_a if word.lower() in string_b_lower)


def get_matching_ids_and_counts(string_a, df, id_col='ID', text_col='string_b'):
    # This function applies count_words_in_string() across all rows in a DataFrame
    # and returns a list of IDs and match counts where at least one match is found, sorted by count (descending).

    if string_a is None or df is None:
        return None  # Guard against invalid inputs

    results = []
    for _, row in df.iterrows():
        text_b = row.get(text_col)
        id_val = row.get(id_col)

        if text_b is None or id_val is None:
            continue  # Skip rows with missing required fields

        count = count_words_in_string(string_a, text_b)
        if count and count > 0:
            results.append((id_val, count))

    if not results:
        return None

    # Sort by count in descending order
    results.sort(key=lambda x: x[1], reverse=True)
    return results


def add_top_n_matched_org_info(vol_df, org_df, n=1, matched_col='MatchedOrg', id_col='ID', title_col='Title', link_col='Permalink'):
    """
    Adds N columns for matched organization titles and permalinks based on IDs in the matched_col.

    Parameters:
        vol_df (pd.DataFrame): DataFrame with volunteer info and matched results
        org_df (pd.DataFrame): DataFrame with organization info
        n (int): Number of top matched organizations to include
        matched_col (str): Column name in vol_df with list of matches (e.g., [(ID, count), ...])
        id_col (str): Column name in org_df representing organization IDs
        title_col (str): Column in org_df with org titles
        link_col (str): Column in org_df with weblinks

    Returns:
        pd.DataFrame: Updated vol_df with new columns for top N matched org titles and permalinks
    """
    # Create lookup dictionaries for fast ID-to-info access
    id_to_title = org_df.set_index(id_col)[title_col].to_dict()
    id_to_link = org_df.set_index(id_col)[link_col].to_dict()

    # Initialize empty columns
    for i in range(1, n + 1):
        vol_df[f'MatchedTitle_{i}'] = None
        vol_df[f'MatchedPermalink_{i}'] = None

    # Fill in the new columns
    for idx, row in vol_df.iterrows():
        matches = row.get(matched_col)
        if matches:
            for i in range(min(n, len(matches))):
                match_id = matches[i][0]  # Get ID from match tuple
                vol_df.at[idx, f'MatchedTitle_{i+1}'] = id_to_title.get(match_id)
                vol_df.at[idx, f'MatchedPermalink_{i+1}'] = id_to_link.get(match_id)

    return vol_df

import pandas as pd

def merge_contact_with_volunteers(contact_csv, vol_excel):
    """
    Merges contact list with volunteer data on email addresses.

    Parameters:
    - contact_csv (str): Path to the contact list CSV file.
    - vol_excel (str): Path to the volunteer Excel file.
    - sep (str): CSV separator used in the contact list.

    Returns:
    - pd.DataFrame: Merged DataFrame with matched titles and permalinks.
    """
    # Load data
    Vol_DF = pd.read_excel(vol_excel)
    ContactList = pd.read_csv(contact_csv,engine='python')

    # Retain only relevant columns
    print(ContactList.info)
    ContactList = ContactList[['CONTACT ID', 'EMAIL']]
    # Perform the merge
    merged_df = ContactList.merge(
        Vol_DF[['User Email', 'MatchedTitle_1', 'MatchedPermalink_1',
                'MatchedTitle_2', 'MatchedPermalink_2']],
        left_on='EMAIL',
        right_on='User Email',
        how='inner'
    )

    return merged_df



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Vol_DF, Org_DF = read_in_data()
    # Vol_DF['MatchedOrg']=Vol_DF['volunteer-causes'].apply(get_matching_ids_and_counts,args=(Org_DF,'ID','Causes'))
    # Vol_DF = add_top_n_matched_org_info(Vol_DF, Org_DF], n=2, matched_col='MatchedOrg', id_col='ID', title_col='Title', link_col='Permalink')
    # Vol_DF.to_excel("Vol_matchedOrg2.xlsx",index=False)
    ContactList = merge_contact_with_volunteers("ContactList45.csv", 'Vol_matchedOrg2.xlsx')

    ContactList.to_excel("UpdatedContactList.xlsx",index=False)

