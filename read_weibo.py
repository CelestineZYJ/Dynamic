import csv
import ast
import re
from tqdm import tqdm
import jieba
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
    if len(jieba.lcut(chinese_text)) < 10:
        return False
    else:
        return chinese_text

def extract_hashtags(string):
    # Define a regular expression pattern to match hashtags
    hashtag_pattern = re.compile(r'#([\u4e00-\u9fff\w]+)#')
    
    # Find all matches of hashtags in the string
    hashtags = hashtag_pattern.findall(string)
    
    return hashtags

def process_user_history_weibo(uid, weibostr):
    new_weibo_list = []
    user_weibo_list = ast.literal_eval(weibostr)
    for each_weibo in user_weibo_list:
        weibo_string = each_weibo[0]
        hashtag_list = extract_hashtags(weibo_string)
        weibo_time = each_weibo[1].split(' ')[0]
        chi_weibo_str = extract_chinese_text(weibo_string)
        if hashtag_list and chi_weibo_str:
            new_weibo_list.append({'timestamp':weibo_time, 'user':uid, 'text':chi_weibo_str, 'tag_list':hashtag_list})
    return new_weibo_list

def read_csv_file(file_path):
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        # Skip the header row
        next(csv_reader)
        for row in tqdm(csv_reader):
            # Assuming the CSV file has three columns: id, weibo, paper_time
            uid, user_all_weibos = row[1], row[2]
            this_user_weibo_list = process_user_history_weibo(uid, user_all_weibos)
            data.extend(this_user_weibo_list)
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

def extract_tag_dict(time_list):
    tag_dict = {}
    for each_weibo in tqdm(time_list):
        timestamp, uid, weibo_text, u_tag_list = each_weibo[0], each_weibo[1], each_weibo[2], each_weibo[3]
        for tag in u_tag_list:
            if tag not in tag_dict:
                tag_dict[tag]={'context_num':0, 'users':{}}
            if uid not in tag_dict[tag]['users']:
                tag_dict[tag]['users'][uid]=[]
            tag_dict[tag]['users'][uid].append({'text':weibo_text, 'timestamp':timestamp})
            tag_dict[tag]['context_num']+=1
    for tag in tag_dict:
        if len(tag_dict[tag]['users'].keys())>100:
            del tag_dict[tag]
            print('delete a tag used by more than 100 users')
    print('totally '+str(len(tag_dict.keys()))+' tags in this subset')
    return tag_dict

def extract_user_dict(time_list):
    user_dict = {}
    for each_weibo in tqdm(time_list):
        timestamp, uid, weibo_text, u_tag_list = each_weibo[0], each_weibo[1], each_weibo[2], each_weibo[3]
        if uid not in user_dict:
            user_dict[uid] = {'weibo_num':0, 'tags':{}}
        for tag in u_tag_list:
            if tag not in user_dict[uid]['tags']:
                user_dict[uid]['tags'][tag]=[]
            user_dict[uid]['tags'][tag].append({'text':weibo_text, 'timestamp':timestamp})
            user_dict[uid]['weibo_num']+=1
    for user in user_dict:
        if len(user_dict[user]['weibo_num'])>100:
            del user_dict[user]
            print('delete a user posting more than 100 weibos')
    print('totally '+str(len(user_dict.keys()))+' users in this subset')
    return user_dict

train_file_path = './weibo_data/train.csv'  # Replace 'sample.csv' with your actual file path
test1_file_path = './weibo_data/test1.csv'
test2_file_path = './weibo_data/test2.csv'
val_file_path = './weibo_data/dev.csv'

# train_ulist = analyze_users(train_file_path)
# test1_ulist = analyze_users(test1_file_path)
# test2_ulist = analyze_users(test2_file_path)
# val_ulists = analyze_users(val_file_path)
# count_shared_elements(train_ulist, test1_ulist, test2_ulist, val_ulists) # 0 shared user in four subsets

all_data = []
train_data = read_csv_file(train_file_path)
all_data.extend(train_data)
test1_data = read_csv_file(test1_file_path)
all_data.extend(test1_data)
test2_data = read_csv_file(test2_file_path)
all_data.extend(test2_data)
val_data = read_csv_file(val_file_path)
all_data.extend(val_data)
print('There are totally '+str(len(all_data))+' weibos with length longer than 10 in the original data')
time_sorted_list = sorted(all_data, key=lambda x: x['timestamp'])

time_sorted_list = time_sorted_list[20000:] # only retain data in a reasonable time span, the list[0]['time']=2009, the list[200000]['time']=2019.12, the list[-1]['time']=2021.01

past_train = time_sorted_list[:int(len(time_sorted_list)*0.5)]
future_train = time_sorted_list[int(len(time_sorted_list)*0.5):int(len(time_sorted_list)*0.8)]
future_test = time_sorted_list[int(len(time_sorted_list)*0.8):]

past_train_tag_context = extract_tag_dict(past_train)
future_train_tag_context = extract_tag_dict(future_train)
future_test_tag_context = extract_tag_dict(future_test)

shared_hastags = list(set(past_train_tag_context.keys())&set(future_train_tag_context.keys())&set(future_test_tag_context.keys()))
print('there are totally '+str(len(shared_hastags))+' hashtags shared by three subsets')

past_train_user_weibo = extract_user_dict(past_train)
future_train_user_weibo = extract_user_dict(future_train)
future_test_user_weibo = extract_user_dict(future_test)

shared_users = list(set(past_train_user_weibo.keys())&set(future_train_user_weibo.keys())&set(future_test_user_weibo.keys()))
print('there are totally '+str(len(shared_users))+' users shared by three subsets')

