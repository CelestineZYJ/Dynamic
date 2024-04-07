import csv
import ast
import re
from tqdm import tqdm
import csv
csv.field_size_limit(100000000)  # Set a larger limit (e.g., 100 MB)

def count_shared_elements(list1, list2, list3, list4):
    # Convert lists to sets
    set1 = set(list1)
    set2 = set(list2)
    set3 = set(list3)
    set4 = set(list4)
    
    # Calculate the intersection of sets
    intersection = set1 & set2 & set3 & set4
    
    # Return the number of shared elements

    print( len(intersection))

def extract_chinese_text(string):
    # Define a regular expression pattern to match Chinese characters
    chinese_pattern = re.compile('[\u4e00-\u9fff]+')
    
    # Find all matches of Chinese characters in the string
    chinese_matches = chinese_pattern.findall(string)
    
    # Join the matches into a single string
    chinese_text = ''.join(chinese_matches)
    
    return chinese_text

def extract_hashtags(string):
    # Define a regular expression pattern to match hashtags
    hashtag_pattern = re.compile(r'#([\u4e00-\u9fff\w]+)#')
    
    # Find all matches of hashtags in the string
    hashtags = hashtag_pattern.findall(string)
    
    return hashtags

def process_user_history_weibo(weibostr):
    new_weibo_list = []
    user_weibo_list = ast.literal_eval(weibostr)
    for each_weibo in user_weibo_list:
        weibo_string = each_weibo[0]
        hashtag_list = extract_hashtags(weibo_string)
        weibo_time = each_weibo[1]
        if hashtag_list:
            chi_weibo_str = extract_chinese_text(weibo_string)
            new_weibo_list.append([chi_weibo_str, hashtag_list, weibo_time])
    return new_weibo_list

# def read_csv_file_by_user(file_path):
#     data = []
#     with open(file_path, 'r', encoding='utf-8') as file:
#         csv_reader = csv.reader(file)
#         # Skip the header row
#         next(csv_reader)
#         for row in csv_reader:
#             # Assuming the CSV file has three columns: id, weibo, paper_time
#             if len(row) == 6:
#                 _, uid, nested_list_str, date, some_value, another_value = row
#             if len(row) == 4:
#                 _, uid, nested_list_str, date = row
#             user_weibo_list = process_user_history_weibo(nested_list_str)
#             data.append({'uid': uid, 'user_weibo':user_weibo_list,'ana_timestamp': date})
#     return data

def read_csv_file(file_path):
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        # Skip the header row
        next(csv_reader)
        for row in csv_reader:
            # Assuming the CSV file has three columns: id, weibo, paper_time
            uid, user_all_weibos, timestamp = row[1], row[2], row[3]
            user_weibo_list = process_user_history_weibo(user_all_weibos)
            data.append({'uid': uid, 'user_weibo':user_weibo_list,'timestamp': timestamp})
    return data

def analyze_users(file_path):
    ulist = []
    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        # Skip the header row
        next(csv_reader)
        for row in tqdm(csv_reader):
            # Assuming the CSV file has three columns: id, weibo, paper_time
            uid=row[1]
            ulist.append(uid)
    return ulist


train_file_path = './weibo_data/train.csv'  # Replace 'sample.csv' with your actual file path
test1_file_path = './weibo_data/test1.csv'
test2_file_path = './weibo_data/test2.csv'
val_file_path = './weibo_data/dev.csv'

# train_ulist = analyze_users(train_file_path)
# test1_ulist = analyze_users(test1_file_path)
# test2_ulist = analyze_users(test2_file_path)
# val_ulists = analyze_users(val_file_path)
# count_shared_elements(train_ulist, test1_ulist, test2_ulist, val_ulists) # 0 shared user in four subsets

train_data = read_csv_file(train_file_path)
for row in train_data[:20]:
    print(row)
