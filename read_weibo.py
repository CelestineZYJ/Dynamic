import csv
import ast
import json
import re
from tqdm import tqdm
import jieba
import random
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
    print(len(intersection))

def extract_chinese_text(string):
    # Define a regular expression pattern to match Chinese characters
    chinese_pattern = re.compile('[\u4e00-\u9fff]+')
    
    # Find all matches of Chinese characters in the string
    chinese_matches = chinese_pattern.findall(string)
    
    # Join the matches into a single string
    chinese_text = ''.join(chinese_matches)
    if len(jieba.lcut(chinese_text)) < 5:
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
            for tag in hashtag_list:
                chi_weibo_str = chi_weibo_str.replace(tag, '')
            new_weibo_list.append({'timestamp':weibo_time, 'user':uid, 'text':chi_weibo_str, 'tag_list':hashtag_list})
    return new_weibo_list

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

def read_csv_file(file_path):
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        # Skip the header row
        next(csv_reader)
        idx = 0
        for row in tqdm(csv_reader):
            # Assuming the CSV file has three columns: id, weibo, paper_time
            if idx < 10000000000:
                uid, user_all_weibos = row[1], row[2]
                this_user_weibo_list = process_user_history_weibo(uid, user_all_weibos)
                data.extend(this_user_weibo_list)
                idx+=1
    return data

def extract_tag_dict(time_list):
    tag_dict = {}
    for each_weibo in tqdm(time_list):
        timestamp, uid, weibo_text, u_tag_list = each_weibo['timestamp'], each_weibo['user'], each_weibo['text'], each_weibo['tag_list']
        for tag in u_tag_list:
            if tag not in tag_dict:
                tag_dict[tag]={'context_num':0, 'users':{}}
            if uid not in tag_dict[tag]['users']:
                tag_dict[tag]['users'][uid]=[]
            tag_dict[tag]['users'][uid].append({'text':weibo_text, 'timestamp':timestamp})
            tag_dict[tag]['context_num']+=1
    print('before filtering, totally '+str(len(tag_dict.keys()))+' tags in this subset')
    for tag in list(tag_dict.keys()):
        if len(tag_dict[tag]['users'].keys())>80 or len(tag_dict[tag]['users'].keys())<3:
            del tag_dict[tag]
            # print('delete a tag used by more than 100 users or less than 3 users')
    print('after filtering, totally '+str(len(tag_dict.keys()))+' tags in this subset')
    return tag_dict

def extract_user_dict(time_list):
    user_dict = {}
    for each_weibo in tqdm(time_list):
        timestamp, uid, weibo_text, u_tag_list = each_weibo['timestamp'], each_weibo['user'], each_weibo['text'], each_weibo['tag_list']
        if uid not in user_dict:
            user_dict[uid] = {'weibo_num':0, 'tags':{}}
        for tag in u_tag_list:
            if tag not in user_dict[uid]['tags']:
                user_dict[uid]['tags'][tag]=[]
            user_dict[uid]['tags'][tag].append({'text':weibo_text, 'timestamp':timestamp})
            user_dict[uid]['weibo_num']+=1
    print('before filtering, totally '+str(len(user_dict.keys()))+' users in this subset')
    for user in list(user_dict.keys()):
        if (user_dict[user]['weibo_num'])>80 or (user_dict[user]['weibo_num'])<3:
            del user_dict[user]
            # print('delete a user posting more than 100 weibos or less than 3 weibos')
    print('after filtering, totally '+str(len(user_dict.keys()))+' users in this subset')
    return user_dict

def keep_shared_tagoruser_subset(shared_hastags, past_train_tag_context, future_train_tag_context, future_test_tag_context):
    new_past_train, new_future_train, new_future_test = {}, {}, {}
    for tag in shared_hastags:
        new_past_train[tag], new_future_train[tag], new_future_test[tag] = past_train_tag_context[tag], future_train_tag_context[tag], future_test_tag_context[tag]
    return new_past_train, new_future_train, new_future_test

def list2string(weibo_list):
    weibo_list = list(set(weibo_list))
    if len(weibo_list)>20:
        weibo_list=random.sample(weibo_list, 20)
    weibo_str = ''
    for weibo in weibo_list:
        weibo_str+=(weibo+'\n')
    return len(weibo_list), weibo_str

def formulate_past_train_set(shared_tags, shared_users, past_train_tag_context, past_train_user_weibo):
    input_past_train_set = []
    for uid in shared_users:
        past_user_weibo = []
        for u_tag in past_train_user_weibo[uid]['tags']:
            weibo_this_tag_list = past_train_user_weibo[uid]['tags'][u_tag]
            for weibo_this_tag in weibo_this_tag_list:
                past_user_weibo.append(weibo_this_tag['text'])
        num_past_user_weibo, past_user_weibo_str = list2string(past_user_weibo)

        pos_tags = []
        for pos_tag in past_train_user_weibo[uid]['tags']:
            if pos_tag in shared_tags:
                this_tag_context = []
                for this_pos_tag_user in past_train_tag_context[pos_tag]['users']:
                    this_pos_tag_user_weibo_list = past_train_tag_context[pos_tag]['users'][this_pos_tag_user]
                    for this_pos_tag_user_weibo in this_pos_tag_user_weibo_list:
                        this_tag_context.append(this_pos_tag_user_weibo['text'])
                num_this_pos_tag_context, this_tag_context_str = list2string(this_tag_context)
                if num_this_pos_tag_context <2:
                    continue
                else:
                    pos_tags.append(pos_tag)
                    input_sentence = 'user history tweets include: '+past_user_weibo_str+' hashtag context tweets include: '+this_tag_context_str
                    input_user_hashtag_interact_pair = {'text':input_sentence, 'label':1, 'reward_score':0, 'time':0,'user_id':uid, 'hashtag':pos_tag, 'category':'past_training', 'user_past_history_num': num_past_user_weibo, 'user_future_history_num':0, 'pos_tag_past_tweets_num':num_this_pos_tag_context, 'pos_tag_future_tweets_num':0}
                    input_past_train_set.append(input_user_hashtag_interact_pair)
        
        # here first select a full set of negative tags pool. to choose from the shared_hashtags is a bit small set, to choose from the whole hashtags is larger
        full_tag_set = list(set(past_train_tag_context.keys())-set(pos_tags))
        neg_tags = random.sample(full_tag_set, len(pos_tags)*20)

        for neg_tag in neg_tags:
            this_neg_tag_context = []
            for this_neg_tag_user in past_train_tag_context[neg_tag]['users']:
                this_neg_tag_weibo_list = past_train_tag_context[neg_tag]['users'][this_neg_tag_user]
                for this_neg_tag_weibo in this_neg_tag_weibo_list:
                    this_neg_tag_context.append(this_neg_tag_weibo['text'])
            num_this_neg_tag_context, this_neg_tag_context_str = list2string(this_neg_tag_context)
            if num_this_neg_tag_context<2:
                continue
            else:
                input_sentence = 'user history tweets include: '+past_user_weibo_str+'hashtag context tweets include: '+this_neg_tag_context_str
                input_user_hashtag_interact_pair = {'text':input_sentence, 'label':0, 'reward_score':0, 'time':0,'user_id':uid, 'hashtag':neg_tag, 'category':'past_training', 'user_past_history_num': num_past_user_weibo, 'user_future_history_num':0, 'neg_tag_past_tweets_num':num_this_neg_tag_context, 'neg_tag_future_tweets_num':0}
                input_past_train_set.append(input_user_hashtag_interact_pair)
    tempf = open('./weibo_data/input_past_training_data.json', 'w', encoding="utf-8")
    tempf.close()  
    with open('./weibo_data/input_past_training_data.json', 'a', encoding="utf-8") as out:
        for l in input_past_train_set:
            json_str=json.dumps(l, ensure_ascii=False)
            out.write(json_str+"\n")

def formulate_future_train_set(shared_tags, shared_users, past_train_tag_context, past_train_user_weibo, future_train_tag_context, future_train_user_weibo, future_test_tag_context, future_test_user_weibo):
    input_future_train_set = []
    for uid in shared_users:
        past_user_weibo = []
        for u_tag in past_train_user_weibo[uid]['tags']:
            weibo_this_tag_list = past_train_user_weibo[uid]['tags'][u_tag]
            for weibo_this_tag in weibo_this_tag_list:
                past_user_weibo.append(weibo_this_tag['text'])
        num_past_user_weibo, past_user_weibo_str = list2string(past_user_weibo)
        future_user_weibo = []
        for u_tag in future_train_user_weibo[uid]['tags']:
            weibo_this_tag_list = future_train_user_weibo[uid]['tags'][u_tag]
            for weibo_this_tag in weibo_this_tag_list:
                future_user_weibo.append(weibo_this_tag['text'])
        num_future_user_weibo, future_user_weibo_str = list2string(future_user_weibo)

        pos_tags = []
        for pos_tag in future_train_user_weibo[uid]['tags']:
            if pos_tag in shared_tags:
                this_tag_past_context = []
                for this_pos_tag_user in past_train_tag_context[pos_tag]['users']:
                    this_pos_tag_user_weibo_list = past_train_tag_context[pos_tag]['users'][this_pos_tag_user]
                    for this_pos_tag_user_weibo in this_pos_tag_user_weibo_list:
                        this_tag_past_context.append(this_pos_tag_user_weibo['text'])
                num_this_pos_tag_past_context, this_tag_past_context_str = list2string(this_tag_past_context)
                if num_this_pos_tag_past_context <2:
                    continue
                this_tag_future_context = []
                for this_pos_tag_user in future_train_tag_context[pos_tag]['users']:
                    this_pos_tag_user_weibo_list = future_train_tag_context[pos_tag]['users'][this_pos_tag_user]
                    for this_pos_tag_user_weibo in this_pos_tag_user_weibo_list:
                        this_tag_future_context.append(this_pos_tag_user_weibo['text'])
                num_this_pos_tag_future_context, this_tag_future_context_str = list2string(this_tag_future_context)
                if num_this_pos_tag_future_context <2:
                    continue
                pos_tags.append(pos_tag)
                input_sentence = 'user past history tweets include: '+past_user_weibo_str+' user future history tweets include: '+future_user_weibo_str+' hashtag past context tweets include: '+this_tag_past_context_str+'  hashtag future context tweets include: '+this_tag_future_context_str
                input_user_hashtag_interact_pair = {'text':input_sentence, 'label':1, 'reward_score':1, 'time':1,'user_id':uid, 'hashtag':pos_tag, 'category':'future_training_with_past_future_user_hashtag_tweets','user_past_history_num': num_past_user_weibo, 'user_future_history_num': num_future_user_weibo, 'pos_tag_past_tweets_num':num_this_pos_tag_past_context, 'pos_tag_future_tweets_num':num_this_pos_tag_future_context}
                input_future_train_set.append(input_user_hashtag_interact_pair)

        full_tag_set = list(set(future_train_tag_context.keys())-set(pos_tags))
        neg_tags = random.sample(full_tag_set, len(pos_tags)*20)
        for neg_tag in neg_tags:
            this_neg_tag_past_context = []
            if neg_tag in past_train_tag_context:
                for this_neg_tag_user in past_train_tag_context[neg_tag]['users']:
                    this_neg_tag_weibo_list = past_train_tag_context[neg_tag]['users'][this_neg_tag_user]
                    for this_neg_tag_weibo in this_neg_tag_weibo_list:
                        this_neg_tag_past_context.append(this_neg_tag_weibo['text'])
                num_this_neg_tag_past_context, this_neg_tag_past_context_str = list2string(this_neg_tag_past_context)
                if num_this_neg_tag_past_context<2:
                    this_neg_tag_past_context_str=''
                    num_this_neg_tag_past_context=0
                    continue
            else:
                this_neg_tag_past_context_str=''
                num_this_neg_tag_past_context=0
            this_neg_tag_future_context = []
            for this_neg_tag_user in future_train_tag_context[neg_tag]['users']:
                this_neg_tag_weibo_list = future_train_tag_context[neg_tag]['users'][this_neg_tag_user]
                for this_neg_tag_weibo in this_neg_tag_weibo_list:
                    this_neg_tag_future_context.append(this_neg_tag_weibo['text'])
            num_this_neg_tag_future_context, this_neg_tag_future_context_str = list2string(this_neg_tag_future_context)
            if num_this_neg_tag_future_context<2:
                continue
            input_sentence = 'user past history tweets include: '+past_user_weibo_str+' user future history tweets include: '+future_user_weibo_str+' hashtag past context tweets include: '+this_neg_tag_past_context_str+'  hashtag future context tweets include: '+this_neg_tag_future_context_str
            input_user_hashtag_interact_pair = {'text':input_sentence, 'label':0, 'reward_score':1, 'time':1,'user_id':uid, 'hashtag':neg_tag, 'category':'future_training_with_past_future_user_hashtag_tweets','user_past_history_num': num_past_user_weibo, 'user_future_history_num': num_future_user_weibo, 'neg_tag_past_tweets_num':num_this_neg_tag_past_context, 'neg_tag_future_tweets_num':num_this_neg_tag_future_context}
            input_future_train_set.append(input_user_hashtag_interact_pair)
    tempf = open('./weibo_data/input_future_training_data.json', 'w', encoding="utf-8")
    tempf.close()  
    with open('./weibo_data/input_future_training_data.json', 'a', encoding="utf-8") as out:
        for l in input_future_train_set:
            json_str=json.dumps(l,ensure_ascii=False)
            out.write(json_str+"\n")   

def formulate_future_test_set(shared_tags, shared_users, past_train_tag_context, past_train_user_weibo, future_train_tag_context, future_train_user_weibo, future_test_tag_context, future_test_user_weibo):
    input_future_test_set = []
    num_new_tag = 0
    num_shared_tag = 0
    for uid in shared_users:
        user_weibo = []
        for u_tag in past_train_user_weibo[uid]['tags']:
            weibo_this_tag_list = past_train_user_weibo[uid]['tags'][u_tag]
            for weibo_this_tag in weibo_this_tag_list:
                user_weibo.append(weibo_this_tag['text'])
        num_past_user_weibo = len(list(set(user_weibo)))
        new_user_weibo = []
        for u_tag in future_train_user_weibo[uid]['tags']:
            weibo_this_tag_list = future_train_user_weibo[uid]['tags'][u_tag]
            for weibo_this_tag in weibo_this_tag_list:
                new_user_weibo.append(weibo_this_tag['text'])
        num_future_user_weibo = len(list(set(new_user_weibo)))
        user_weibo.extend(new_user_weibo)
        num_user_weibo, user_weibo_str = list2string(user_weibo)

        pos_tags = []
        for pos_tag in future_test_user_weibo[uid]['tags']:    
            if pos_tag in shared_hastags:
            # if 1: # during inference, test both cold-start hashtags and shared_hashtags
                this_tag_context = []
                if pos_tag in past_train_tag_context:
                    for this_pos_tag_user in past_train_tag_context[pos_tag]['users']:
                        this_pos_tag_user_weibo_list = past_train_tag_context[pos_tag]['users'][this_pos_tag_user]
                        for this_pos_tag_user_weibo in this_pos_tag_user_weibo_list:
                            this_tag_context.append(this_pos_tag_user_weibo['text'])
                if pos_tag in future_train_tag_context:
                    for this_pos_tag_user in future_train_tag_context[pos_tag]['users']:
                        this_pos_tag_user_weibo_list = future_train_tag_context[pos_tag]['users'][this_pos_tag_user]
                        for this_pos_tag_user_weibo in this_pos_tag_user_weibo_list:
                            this_tag_context.append(this_pos_tag_user_weibo['text'])
                num_this_pos_tag_context, this_tag_context_str = list2string(this_tag_context)
            
                this_tag_future_test_context = []
                if pos_tag not in future_test_tag_context:
                    continue
                for this_pos_tag_user in future_test_tag_context[pos_tag]['users']:
                    if this_pos_tag_user != uid:
                        this_pos_tag_user_weibo_list = future_test_tag_context[pos_tag]['users'][this_pos_tag_user]
                        for this_pos_tag_user_weibo in this_pos_tag_user_weibo_list:
                            this_tag_future_test_context.append(this_pos_tag_user_weibo['text'])
                num_this_tag_future_test_context, this_tag_future_test_context_str = list2string(this_tag_future_test_context)
                if num_this_tag_future_test_context <2:
                    continue
                pos_tags.append(pos_tag)
                if pos_tag in shared_tags:
                    num_shared_tag+=1
                else:
                    num_new_tag+=1
                input_sentence = 'user history tweets include: '+user_weibo_str+' hashtag past context tweets include: '+this_tag_context_str+' hashtag future context tweets include: '+this_tag_future_test_context_str
                input_user_hashtag_interact_pair = {'text':input_sentence, 'label':1, 'reward_score':1, 'time':1,'user_id':uid, 'hashtag':pos_tag,'user_past_history_num': num_past_user_weibo, 'user_future_history_num': num_future_user_weibo,'pos_tag_past_tweets_num':num_this_pos_tag_context, 'pos_tag_future_tweets_num':num_this_tag_future_test_context}
                input_future_test_set.append(input_user_hashtag_interact_pair)

        full_tag_set = list(set(future_test_tag_context.keys())-set(pos_tags))
        neg_tags = random.sample(full_tag_set, len(pos_tags)*20)
        for neg_tag in neg_tags:
            this_neg_tag_past_context = []
            if neg_tag in past_train_tag_context:
                    for this_neg_tag_user in past_train_tag_context[neg_tag]['users']:
                        this_neg_tag_user_weibo_list = past_train_tag_context[neg_tag]['users'][this_neg_tag_user]
                        for this_neg_tag_user_weibo in this_neg_tag_user_weibo_list:
                            this_neg_tag_past_context.append(this_neg_tag_user_weibo['text'])
            if neg_tag in future_train_tag_context:
                for this_neg_tag_user in future_train_tag_context[neg_tag]['users']:
                    this_neg_tag_user_weibo_list = future_train_tag_context[neg_tag]['users'][this_neg_tag_user]
                    for this_neg_tag_user_weibo in this_neg_tag_user_weibo_list:
                        this_neg_tag_past_context.append(this_neg_tag_user_weibo['text'])
            num_this_neg_tag_past_context, this_neg_tag_past_context_str = list2string(this_neg_tag_past_context)

            this_neg_tag_future_test_context = []
            for this_neg_tag_user in future_test_tag_context[neg_tag]['users']:
                if this_neg_tag_user != uid:
                    this_neg_tag_user_weibo_list = future_test_tag_context[neg_tag]['users'][this_neg_tag_user]
                    for this_neg_tag_user_weibo in this_neg_tag_user_weibo_list:
                        this_neg_tag_future_test_context.append(this_neg_tag_user_weibo['text'])
            num_this_neg_tag_future_test_context, this_neg_tag_future_test_context_str = list2string(this_neg_tag_future_test_context)
            if num_this_neg_tag_future_test_context <2:
                continue
            input_sentence = 'user history tweets include: '+user_weibo_str+' hashtag past context tweets include: '+this_neg_tag_past_context_str+' hashtag future context tweets include: '+this_neg_tag_future_test_context_str
            input_user_hashtag_interact_pair = {'text':input_sentence, 'label':0, 'reward_score':1, 'time':1,'user_id':uid, 'hashtag':neg_tag,'user_past_history_num': num_past_user_weibo, 'user_future_history_num': num_future_user_weibo,'neg_tag_past_tweets_num':num_this_neg_tag_past_context, 'neg_tag_future_tweets_num':num_this_neg_tag_future_test_context}
            input_future_test_set.append(input_user_hashtag_interact_pair)
    print(str(num_new_tag)+' new hashtags in future test data only')
    print(str(num_shared_tag)+' shared hashtags in all three subsets')
    tempf = open('./weibo_data/input_future_test_data.json', 'w', encoding="utf-8")
    tempf.close()
    with open('./weibo_data/input_future_test_data.json', 'a', encoding="utf-8") as out:
        for l in input_future_test_set:
            json_str=json.dumps(l,ensure_ascii=False)
            out.write(json_str+"\n")  

train_file_path = './raw_weibo_data/train.csv'  # Replace 'sample.csv' with your actual file path
test1_file_path = './raw_weibo_data/test1.csv'
test2_file_path = './raw_weibo_data/test2.csv'
val_file_path = './raw_weibo_data/dev.csv'

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
# past_train_tag_context, future_train_tag_context, future_test_tag_context=keep_shared_tagoruser_subset(shared_hastags, past_train_tag_context, future_train_tag_context, future_test_tag_context)


past_train_user_weibo = extract_user_dict(past_train)
future_train_user_weibo = extract_user_dict(future_train)
future_test_user_weibo = extract_user_dict(future_test)

shared_users = list(set(past_train_user_weibo.keys())&set(future_train_user_weibo.keys())&set(future_test_user_weibo.keys()))
print('there are totally '+str(len(shared_users))+' users shared by three subsets')
# past_train_user_weibo, future_train_user_weibo, future_test_user_weibo = keep_shared_tagoruser_subset(shared_users, past_train_user_weibo,future_train_user_weibo,future_test_user_weibo)
shared_users = shared_users[:286]
formulate_past_train_set(shared_hastags, shared_users, past_train_tag_context, past_train_user_weibo)
formulate_future_train_set(shared_hastags, shared_users, past_train_tag_context, past_train_user_weibo, future_train_tag_context, future_train_user_weibo, future_test_tag_context, future_test_user_weibo)
formulate_future_test_set(shared_hastags,shared_users, past_train_tag_context, past_train_user_weibo, future_train_tag_context, future_train_user_weibo, future_test_tag_context, future_test_user_weibo)

