import os
import json
import gzip
from tqdm import tqdm
import copy
import re
import string 
import random
random.seed(2024)


def parse_gz(gz_path):
    sec_dic_list = gzip.open(gz_path, 'rb')
    for sec in sec_dic_list:
        yield json.loads(sec)


def filter_out_hashtag(sec_dict):
    for hashtag in sec_dict['entities']['hashtags']:
        if hashtag['text'].encode('UTF-8').isalnum() and len(hashtag['text']) >= 5:
            continue
        else:
            return 0
    return 1

# # number of day_files = 30(day) * months
# # number of sec_files = 30(day) * months * secs(about 1440 per day)
# def read_day(day_dir):
#     sec_files=os.listdir(day_dir)
#     sec_files.sort()
#     print('secs in one day of '+str(day_dir)+' totally '+str(len(sec_files))+' lines: '+str(sec_files[:3])+'...including all tweets in one day')
    
#     all_secs = []
#     orinigal_sec_num = 0
#     for sec_f in tqdm(sec_files):
#         if str(sec_f)[0]=='2':
#             # sec is one line in sec_f (there are about 1440 lines (secs) in each sec_f)
#             for sec in parse_gz(day_dir+'/'+sec_f):
#                 orinigal_sec_num+=1
#                 if sec['entities']['hashtags'] and not(sec['retweeted']):
#                     exist_tag_each_tweet = filter_out_hashtag(sec)
#                     if exist_tag_each_tweet == 1:
#                         sec['timestamp']=str(sec_f)[:8]
#                         all_secs.append(sec)
#     print('There are '+str(len(all_secs))+' tweets with hashtags. The original tweets number in the day is '+str(orinigal_sec_num)+'. The ratio is '+str(len(all_secs)/orinigal_sec_num))
        
#     f=open('test.json','w')
#     f.writelines(str(all_secs))
        

# number of day_files = 30(day) * months
# number of sec_files = 30(day) * months * secs(about 1440 per day)
def read_day(day_dir):
    user_tweet_dict = {}
    sec_files=os.listdir(day_dir)
    sec_files.sort()
    print('secs in one day of '+str(day_dir)+' totally '+str(len(sec_files))+' lines: '+str(sec_files[:3])+'...including all tweets in one day')
    
    each_day_all_tweets = 0
    hashtag_num = 0
    all_tweets_num = 0
    print(len(sec_files))
    for sec_f in tqdm(sec_files):
        if str(sec_f)[0]=='2':
            # sec is one line in sec_f (there are about 1440 lines (secs) in each sec_f)
            for sec in parse_gz(day_dir+'/'+sec_f):
                all_tweets_num+=1
                if sec['entities']['hashtags'] and not(sec['retweeted']) and pre_filter_tweets(sec):
                    exist_tag_each_tweet = filter_out_hashtag(sec)
                    
                    if exist_tag_each_tweet:
                        sec['timestamp']=str(sec_f)[:8]
                        try:
                            if sec not in user_tweet_dict[sec['user']['id_str']]['tweet_entities']:
                                user_tweet_dict[sec['user']['id_str']]['tweet_entities'].append(sec)
                                user_tweet_dict[sec['user']['id_str']]['user_tweet_num']+=1
                                hashtag_num+=exist_tag_each_tweet 
                            else:
                                continue
                        except:
                            user_tweet_dict[sec['user']['id_str']]={'tweet_entities':[sec], 'user_tweet_num':1}
                            hashtag_num+=exist_tag_each_tweet 
                        each_day_all_tweets+=1
                            
                                   
    print('The ratio of tweets with hashtags in all tweets in one day is: '+str(hashtag_num/all_tweets_num))
    print('Day of '+str(day_dir)+' has '+str((each_day_all_tweets))+' tweets')
    
    return user_tweet_dict
    

def filter_user_with_enough_tweets(user_tweet_dict, filtered_user_tweet_dict):
    # num_filtered_user_tweet_dict = {}
    num_filtered_users_tweets = 0
    
    for user_id in user_tweet_dict.keys():
        if user_tweet_dict[user_id]['user_tweet_num']>=3 and user_tweet_dict[user_id]['user_tweet_num']<=40: # 1, 2: 5-na, 3, 4: 6-100, 5: 60, 6:8-50
            # random sample
            # if random.randint(1,10)<=2:
            filtered_user_tweet_dict[user_id]=user_tweet_dict[user_id]
            num_filtered_users_tweets+=filtered_user_tweet_dict[user_id]['user_tweet_num']
            # num_filtered_user_tweet_dict[user_id]=user_tweet_dict[user_id]['user_tweet_num']

    # print(num_filtered_user_tweet_dict) 
    print('There are totally '+str(len(filtered_user_tweet_dict))+' users who have 3 more tweets in one month, with totally '+str(num_filtered_users_tweets)+' tweets')     
    print('The original number of users in these days is: '+str(len(user_tweet_dict)))
    
    return filtered_user_tweet_dict

def filter_user_with_proper_hashtags(filtered_user_tweet_dict):
    hashtag_num_dict = {}
    new_hashtag_user_dict = {}
    new_hashtag_list=[]
    for user_id in tqdm(filtered_user_tweet_dict.keys(), desc='constructing the hashtag_num_dict '):
        for each_tweet in filtered_user_tweet_dict[user_id]['tweet_entities']:
            if pre_filter_tweets(each_tweet):
                hashtags = each_tweet['entities']['hashtags']
                for hashtag in hashtags:
                    hashtag['text'] = hashtag['text'].lower()
                    try:
                        hashtag_num_dict[hashtag['text']]+=1
                        new_hashtag_user_dict[hashtag['text']].append(user_id)
                        new_hashtag_user_dict[hashtag['text']]=list(set(new_hashtag_user_dict[hashtag['text']]))
                    except:
                        hashtag_num_dict[hashtag['text']]=1
                        new_hashtag_user_dict[hashtag['text']] = [user_id]
    print('the total number of the original hashtags is: '+str(len(hashtag_num_dict)))

    filter_user_hashtag_tweet_dict={}
    for user_id in tqdm(filtered_user_tweet_dict.keys(), desc='delete tweets with low-source hashtags for all users'):
        new_history_tweets = []
        for each_tweet in filtered_user_tweet_dict[user_id]['tweet_entities']:
            if pre_filter_tweets(each_tweet):
                hashtags = copy.deepcopy(each_tweet['entities']['hashtags']) # only copy the value without the handle
                new_proper_hashtags = []
                for hashtag in hashtags:
                    hashtag['text'] = hashtag['text'].lower()
                    if hashtag_num_dict[hashtag['text']]>=3 and len(hashtag['text'])>=5 and hashtag_num_dict[hashtag['text']]<=500 and len(new_hashtag_user_dict[hashtag['text']])>=3 and len(new_hashtag_user_dict[hashtag['text']]) <= 500:
                        new_hashtag_list.append(hashtag['text'])
                        new_proper_hashtags.append(hashtag)
                        each_tweet['entities']['hashtags']=new_proper_hashtags
                        if each_tweet not in new_history_tweets:
                            new_history_tweets.append(each_tweet)
                    
        if len(new_history_tweets) >= 3 and len(new_history_tweets) <= 50:
            # if len(new_history_tweets)>30:
            #     new_history_tweets = random.sample(new_history_tweets, 30)
            filter_user_hashtag_tweet_dict[user_id]=new_history_tweets
                 
    print('the total number of filtered users with proper hashtags and 3 more history tweets in one month is: '+str(len(filter_user_hashtag_tweet_dict)))
    print('the total number of the filtered hashtags is: '+str(len(list(set(new_hashtag_list)))))
    return filter_user_hashtag_tweet_dict


def slim_tweet_form(each_day_user_tweet_dict):
    slim_each_day_user_tweet_dict = {}
    for user_id in each_day_user_tweet_dict:
        slim_each_day_user_tweet_dict[user_id]={'tweet_entities':[]}
        for each_tweet in each_day_user_tweet_dict[user_id]['tweet_entities']:
            each_slim_tweet = {key:val for key, val in each_tweet.items() if (key=='created_at' or key=='id_str' or key=='text' or key=='source' or key=='entities' or key=='timestamp')}
            # print('*'*100)
            # print(each_slim_tweet.keys())
            slim_each_day_user_tweet_dict[user_id]['tweet_entities'].append(each_slim_tweet)
    
    return slim_each_day_user_tweet_dict


def write_dict_to_json_file(filtered_user_tweet_dict, mon):
    filter_user_dict_obj = json.dumps(filtered_user_tweet_dict)
    filter_user_file = open('middle_processed_data/'+str(mon)+'.json', 'w')
    filter_user_file.write(filter_user_dict_obj)
    filter_user_file.close()
    
    
def read_json_file_to_dict(dict_name):
    filter_user_file = open('middle_processed_data/'+str(dict_name)+'.json', 'r')
    filter_user_dict_obj = filter_user_file.read()
    filtered_user_tweet_dict = json.loads(filter_user_dict_obj)
    filter_user_file.close()
    return filtered_user_tweet_dict


def pre_filter_tweets(each_tweet):
    tweet_str = each_tweet['text']
    hashtag_list = []
    if tweet_str[:2] != 'RT':
        # print('*'*100)
        # print(tweet_str)
        new_str = re.sub(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*', '', tweet_str).lower()
        extracted_hashtags = re.findall(r'\#\w+', new_str)
        for ext_tag in extracted_hashtags:
            hashtag_list.append(ext_tag.replace('#', ''))
        hashtag_list = list(set(hashtag_list))
        for tag in hashtag_list:
            new_str = re.sub('#'+tag, '', new_str)
        new_str = re.sub('\t', ' ', new_str)
        new_str = re.sub('\n', ' ', new_str)
        for i in string.punctuation:
            if i != '@':
                new_str = new_str.replace(i, '')
        for i in new_str:
            if not i.encode('utf-8').isalnum() and i != ' ' and i != '@':
                new_str = new_str.replace(i, '')
        new_str = new_str.replace('  ', ' ')
        new_str = re.sub('  ', '', new_str)
        if len(new_str.split(' '))-new_str.count('@')>=5:
            return True
            # print('*'*100)
            # print(tweet_str)
            # print('&'*100)
            # print(new_str)
            # print(hashtag_list)
        else:
            return False
    else:
        return False


def filter_meaningful_tweet(tweet_str, hashtag_list):
    filtered_tweet = ''
    if tweet_str[:2] != 'RT':
        # print('*'*100)
        # print(tweet_str)
        new_str = re.sub(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*', '', tweet_str).lower()
        extracted_hashtags = re.findall(r'\#\w+', new_str)
        for ext_tag in extracted_hashtags:
            hashtag_list.append(ext_tag.replace('#', ''))
        hashtag_list = list(set(hashtag_list))
        for tag in hashtag_list:
            new_str = re.sub('#'+tag, '', new_str)
        new_str = re.sub('\t', ' ', new_str)
        new_str = re.sub('\n', ' ', new_str)
        for i in string.punctuation:
            if i != '@':
                new_str = new_str.replace(i, '')
        new_str = re.sub(r'\\u*', '', new_str)
        for i in new_str:
            if not i.encode('utf-8').isalnum() and i != ' ' and i != '@':
                new_str = new_str.replace(i, '')
        new_str = re.sub('   *', '', new_str)
        new_str = new_str.replace('  ', ' ')
        if len(new_str.split(' '))-new_str.count('@')>=10:
            filtered_tweet = new_str.replace('@', '')
            # print('*'*100)
            # print(tweet_str)
            # print('&'*100)
            # print(new_str)
            # print(hashtag_list)
        else:
            filtered_tweet = False
    return filtered_tweet, hashtag_list


def calculte_tweet_number(user_aggre_info_dict):
    tweet_num = 0
    for user_id in user_aggre_info_dict:
        for tag in user_aggre_info_dict[user_id]:
            tweet_num+=len(user_aggre_info_dict[user_id][tag])
    return tweet_num


def construct_hashtag_user_interact_dict(new_past_train_tweets):
    past_train_user_aggre_info = {}
    past_train_hashtag_aggre_info = {}
    for tweet in tqdm(new_past_train_tweets):
        timestamp, tweet_id, user_id, filtered_tweet, tag = tweet.strip('\n').split('\t')[0], tweet.strip('\n').split('\t')[1], tweet.strip('\n').split('\t')[2], tweet.strip('\n').split('\t')[3], tweet.strip('\n').split('\t')[4]
        # construct user_hashtag_interact_dict: past_train_user_aggre_info
        if user_id in past_train_user_aggre_info:
            if tag in past_train_user_aggre_info[user_id]:
                try:
                    past_train_user_aggre_info[user_id][tag].append(tweet)
                    yes_num+=1
                except:
                    past_train_user_aggre_info[user_id][tag] = [tweet]
            else:
                try:
                    past_train_user_aggre_info[user_id][tag] = [tweet]
                except:
                    past_train_user_aggre_info[user_id] = {tag: [tweet]}
        else:
            past_train_user_aggre_info[user_id] = {tag: [tweet]}
        
        # construct hashtag_user_interact_dict: past_train_hashtag_aggre_info
        if tag in past_train_hashtag_aggre_info:
            if user_id in past_train_hashtag_aggre_info[tag]:
                try:
                    past_train_hashtag_aggre_info[tag][user_id].append(tweet)
                    no_num+=1
                except:
                    past_train_hashtag_aggre_info[tag][user_id] = [tweet]
            else:
                try:
                    past_train_hashtag_aggre_info[tag][user_id] = [tweet]
                except:
                    past_train_hashtag_aggre_info[tag] = {user_id: [tweet]}
        else:
            past_train_hashtag_aggre_info[tag] = {user_id: [tweet]}

    ori_tweet_num = calculte_tweet_number(past_train_user_aggre_info)
    print('there are totally '+str(len(past_train_user_aggre_info))+' users')
    print('there are totally '+str(len(past_train_hashtag_aggre_info))+' hashtags')
    print('there are totally '+str(ori_tweet_num)+' tweets')

    # after aggregating user_hashtag_interact_dict and hashtag_user_interact_dict, filter out hashtags used by less than 3 users
    del_tag_num = 0
    del_user_num = 0
    user_aggre_dict_del_uid = []
    new_past_train_hashtag_aggre_info = past_train_hashtag_aggre_info.copy()
    for tag in new_past_train_hashtag_aggre_info:
        if len(new_past_train_hashtag_aggre_info[tag]) < 3 or len(new_past_train_hashtag_aggre_info[tag]) > 50:
            del_tag_num+=1
            del past_train_hashtag_aggre_info[tag]
            for user_id in new_past_train_hashtag_aggre_info[tag]:
                if user_id in past_train_user_aggre_info:
                    del past_train_user_aggre_info[user_id][tag]                
                    # if len(past_train_user_aggre_info[user_id]) < 3:
                    #     user_aggre_dict_del_uid.append(user_id)
                    #     del past_train_user_aggre_info[user_id]
                    #     del_user_num+=1
                        
    new_past_train_user_aggre_info = past_train_user_aggre_info.copy()
    for user_id in new_past_train_user_aggre_info:
        if len(past_train_user_aggre_info[user_id]) < 3:
            user_aggre_dict_del_uid.append(user_id)
            del past_train_user_aggre_info[user_id]
            del_user_num+=1
            
    new_tweet_num = calculte_tweet_number(past_train_user_aggre_info)
    print('\ndelete '+str(del_tag_num)+' hashtags')
    print('\ndelete '+str(del_user_num)+' users')
    print('delete '+str(ori_tweet_num-new_tweet_num)+' tweets\n')
    print('there are totally '+str(len(past_train_user_aggre_info))+' users')
    print('there are totally '+str(len(past_train_hashtag_aggre_info))+' hashtags')
    print('there are totally '+str(new_tweet_num)+' tweets\n')

    return past_train_user_aggre_info, past_train_hashtag_aggre_info, user_aggre_dict_del_uid


def user_hashtag_aggre_partition():
    all_tweets_first = []
    # mon 1, 2, 3, 4, 5, 6
    all_count = 0
    real_count = 0
    # go through and filter all the tweets, give them timestamps for later sequencing
    for mon in range(1, 7):
        filter_user_hashtag_tweet_dict = read_json_file_to_dict('second_filter_user_proper_tag_enough_tweets_mon'+str(mon))
        for user in tqdm(filter_user_hashtag_tweet_dict.keys()):
            for each_tweet in filter_user_hashtag_tweet_dict[user]:
                all_count+=1
                tweet_str = each_tweet['text']
                hashtag_list = each_tweet['entities']['hashtags']
                hashtag_list = [hashtag['text'] for hashtag in hashtag_list]
                filtered_tweet, hashtag_list = filter_meaningful_tweet(tweet_str, hashtag_list)
                if filtered_tweet:
                    user_id = user
                    tweet_id = each_tweet['id_str']
                    timestamp = each_tweet['timestamp']
                    real_count += 1
                    for tag in hashtag_list:
                        all_tweets_first.append(timestamp+'\t'+tweet_id+'\t'+user_id+'\t'+filtered_tweet+'\t'+tag+'\n')
                else:
                    continue
    print('filtered tweets count: '+str(len(all_tweets_first)))
    print('filtered tweets count without duplication: '+str(real_count))
    print('previous all count of tweets: '+str(all_count))
    
    # sequencing all tweets by timestamps and split the train, validation, and test set, keep the sharing user list
    all_tweets_first.sort()
    filtered_tweets_num = len(all_tweets_first)
    past_train_tweets = all_tweets_first[0:int(0.5*filtered_tweets_num)]
    future_train_tweets = all_tweets_first[int(0.5*filtered_tweets_num):int(0.8*filtered_tweets_num)]
    future_test_tweets = all_tweets_first[int(0.8*filtered_tweets_num):]
    
    # firstly, keep a sharing user list, keep corresponding tweets
    past_train_user_list = []
    for tweet in tqdm(past_train_tweets):
        user_id = tweet.strip('\n').split('\t')[2]
        past_train_user_list.append(user_id)
    past_train_user_list = list(set(past_train_user_list))
    
    future_train_user_list = []
    for tweet in tqdm(future_train_tweets):
        user_id = tweet.strip('\n').split('\t')[2]
        future_train_user_list.append(user_id)
    future_train_user_list = list(set(future_train_user_list))
    
    future_test_user_list = []
    for tweet in tqdm(future_test_tweets):
        user_id = tweet.strip('\n').split('\t')[2]
        future_test_user_list.append(user_id)
    future_test_user_list = list(set(future_test_user_list))
    
    all_user_list = list(set(past_train_user_list + future_train_user_list + future_test_user_list))
    sharing_user_list = []
    for user in all_user_list:
        if user in past_train_user_list and user in future_train_user_list and user in future_test_user_list:
            sharing_user_list.append(user)
    
    new_past_train_tweets = []
    for tweet in tqdm(past_train_tweets):
        user_id = tweet.strip('\n').split('\t')[2]
        if user_id in sharing_user_list:
            new_past_train_tweets.append(tweet)
            
    new_future_train_tweets = []
    for tweet in tqdm(future_train_tweets):
        user_id = tweet.strip('\n').split('\t')[2]
        if user_id in sharing_user_list:
            new_future_train_tweets.append(tweet)
            
    new_future_test_tweets = []
    for tweet in tqdm(future_test_tweets):
        user_id = tweet.strip('\n').split('\t')[2]
        if user_id in sharing_user_list:
            new_future_test_tweets.append(tweet)
            
    print('all user number is: '+str(len(sharing_user_list)))
    print('all tweets number is: '+str(len(new_past_train_tweets)+len(new_future_train_tweets)+len(new_future_test_tweets)))
    
    # secondly, create the user_hashtag_interact_dict and the hashtag_user_interact_dict, keep tweets with hashtags used >= 3 users , then give tweet_id, user_id, hashtag_id

    past_train_user_aggre_info, past_train_hashtag_aggre_info, del_past_train_uid_list = construct_hashtag_user_interact_dict(new_past_train_tweets)
    future_train_user_aggre_info, future_train_hashtag_aggre_info, del_future_train_uid_list = construct_hashtag_user_interact_dict(new_future_train_tweets)
    future_test_user_aggre_info, future_test_hashtag_aggre_info, del_future_test_uid_list = construct_hashtag_user_interact_dict(new_future_test_tweets)
    all_del_uid_list = list(set(del_past_train_uid_list+del_future_train_uid_list+del_future_test_uid_list))
    del_uid_in_user_aggre_dict(past_train_user_aggre_info, all_del_uid_list)
    del_uid_in_user_aggre_dict(future_train_user_aggre_info, all_del_uid_list)
    del_uid_in_user_aggre_dict(future_test_user_aggre_info, all_del_uid_list)
    
    write_dict_to_json_file(past_train_user_aggre_info, 'past_train_user_aggre_info_dict')
    write_dict_to_json_file(past_train_hashtag_aggre_info, 'past_train_hashtag_aggre_info_dict')
    write_dict_to_json_file(future_train_user_aggre_info, 'future_train_user_aggre_info_dict')
    write_dict_to_json_file(future_train_hashtag_aggre_info, 'future_train_hashtag_aggre_info_dict')
    write_dict_to_json_file(future_test_user_aggre_info, 'future_test_user_aggre_info_dict')
    write_dict_to_json_file(future_test_hashtag_aggre_info, 'future_test_hashtag_aggre_info_dict')


def del_uid_in_user_aggre_dict(user_aggre_info, all_del_uid_list):
    new_user_aggre_info = user_aggre_info.copy()
    del_user_num = 0
    for user_id in new_user_aggre_info:
        if user_id in all_del_uid_list:
            del user_aggre_info[user_id]
            del_user_num+=1
            
    print('delete '+str(del_user_num)+' users to keep consistent for all subsets')
    print('there are totally '+str(len(user_aggre_info))+' users in user_hashtag_aggre_interact_dict')


def process_sort_tweets_by_time(tweets_list):
    sorted_tweets = ''
    tweets_list = list(set(tweets_list))
    if len(tweets_list)>15:
        tweets_list = random.sample(tweets_list, 15)
    tweets_list.sort()
    filtered_tweet_list = []
    for tweet in tweets_list:
        timestamp, tweet_id, user_id, filtered_tweet, tag = tweet.strip('\n').split('\t')[0], tweet.strip('\n').split('\t')[1], tweet.strip('\n').split('\t')[2], tweet.strip('\n').split('\t')[3], tweet.strip('\n').split('\t')[4]
        if filtered_tweet not in filtered_tweet_list and len(filtered_tweet_list)<=15:
            filtered_tweet_list.append(filtered_tweet)
    
    for tweet in filtered_tweet_list:
        sorted_tweets+=(tweet+'\n')
    return sorted_tweets
        

def formulate_llm_input_past_train(user_list, past_train_user_aggre_info, past_train_hashtag_aggre_info):
    input_past_train_set = []
    # for each user, establish many user-hashtag pairs
    for global_user_id in user_list:
        # input_past_train_set.append('\nuser_id: '+user_id+'\n')
        # acquire this user's history tweets
        user_history_tweets = []
        for tag in past_train_user_aggre_info[global_user_id]:
            tweets_list = past_train_user_aggre_info[global_user_id][tag]
            user_history_tweets = list(set(user_history_tweets+tweets_list))
        user_history_tweets_str = process_sort_tweets_by_time(user_history_tweets)
        
        # establish thisUser-manyHashtag pairs: positive samples of user-hashtag interact pairs
        positive_tags = []
        for tag in past_train_user_aggre_info[global_user_id]:    
            positive_tags.append(tag)
            # acquire all hashtag contexts from users who used it
            pos_hashtag_context_tweets = []
            for user_id in past_train_hashtag_aggre_info[tag]:
                pos_tag_context_tweets_from_each_user_list = past_train_hashtag_aggre_info[tag][user_id]
                pos_hashtag_context_tweets = list(set(pos_hashtag_context_tweets+pos_tag_context_tweets_from_each_user_list))
            pos_hashtag_context_tweets_str = process_sort_tweets_by_time(pos_hashtag_context_tweets)
            input_sentence = 'user history tweets include: '+user_history_tweets_str+'hashtag context tweets include: '+pos_hashtag_context_tweets_str
            input_user_hashtag_interact_pair = {'text':input_sentence, 'label':1, 'user_id':global_user_id, 'hashtag':tag}
            input_past_train_set.append(input_user_hashtag_interact_pair)
        
        # establish thisUser-manyHashtag pairs: negative samples of user-hashtag interact pairs
        # sample negative hashtags of ten times number of postive hashtags
        negative_tags = random.sample(list(set(list(past_train_hashtag_aggre_info.keys()))-set(positive_tags)), 10*len(positive_tags))
        for tag in negative_tags:
            neg_hashtag_context_tweets = []
            for user_id in past_train_hashtag_aggre_info[tag]:
                neg_tag_context_tweets_from_each_user_list = past_train_hashtag_aggre_info[tag][user_id]
                neg_hashtag_context_tweets = list(set(neg_hashtag_context_tweets+neg_tag_context_tweets_from_each_user_list))
            neg_hashtag_context_tweets_str = process_sort_tweets_by_time(neg_hashtag_context_tweets)
            input_sentence = 'user history tweets include: '+user_history_tweets_str+'hashtag context tweets include: '+neg_hashtag_context_tweets_str
            input_user_hashtag_interact_pair = {'text':input_sentence, 'label':0, 'user_id':global_user_id, 'hashtag':tag}
            input_past_train_set.append(input_user_hashtag_interact_pair)
    
    tempf = open('dataset/input_past_training_data.json', 'w')
    tempf.close()  
    with open('dataset/input_past_training_data.json', 'a') as out:
        for l in input_past_train_set:
            json_str=json.dumps(l)
            out.write(json_str+"\n")   
            

def formulate_llm_input_past_future_train(user_list, past_train_user_aggre_info, past_train_hashtag_aggre_info, future_train_user_aggre_info, future_train_hashtag_aggre_info):
    input_past_train_set = []
    # for each user, establish many user-hashtag pairs
    for global_user_id in user_list:
        # input_past_train_set.append('\nuser_id: '+user_id+'\n')
        # acquire this user's history tweets
        past_train_user_history_tweets = []
        for tag in past_train_user_aggre_info[global_user_id]:
            tweets_list = past_train_user_aggre_info[global_user_id][tag]
            past_train_user_history_tweets = list(set(past_train_user_history_tweets+tweets_list))
        past_train_user_history_tweets_str = process_sort_tweets_by_time(past_train_user_history_tweets)
        future_train_user_history_tweets = []
        for tag in future_train_user_aggre_info[global_user_id]:
            tweets_list = future_train_user_aggre_info[global_user_id][tag]
            future_train_user_history_tweets = list(set(future_train_user_history_tweets+tweets_list))
        future_train_user_history_tweets_str = process_sort_tweets_by_time(future_train_user_history_tweets)
        
        # establish thisUser-manyHashtag pairs: positive samples of user-hashtag interact pairs
        positive_tags = []
        for tag in past_train_user_aggre_info[global_user_id]:    
            positive_tags.append(tag)
            # acquire all hashtag contexts from users who used it
            pos_past_train_hashtag_context_tweets = []
            for user_id in past_train_hashtag_aggre_info[tag]:
                pos_past_train_tag_context_tweets_from_each_user_list = past_train_hashtag_aggre_info[tag][user_id]
                pos_past_train_hashtag_context_tweets = list(set(pos_past_train_hashtag_context_tweets+pos_past_train_tag_context_tweets_from_each_user_list))
            pos_past_train_hashtag_context_tweets_str = process_sort_tweets_by_time(pos_past_train_hashtag_context_tweets)
            pos_future_train_hashtag_context_tweets = []
            for user_id in past_train_hashtag_aggre_info[tag]:
                pos_future_train_tag_context_tweets_from_each_user_list = past_train_hashtag_aggre_info[tag][user_id]
                pos_past_train_hashtag_context_tweets = list(set(pos_future_train_hashtag_context_tweets+pos_future_train_tag_context_tweets_from_each_user_list))
            pos_future_train_hashtag_context_tweets_str = process_sort_tweets_by_time(pos_past_train_hashtag_context_tweets)
            input_sentence = 'user past history tweets include: '+past_train_user_history_tweets_str+' user future history tweets include: '+future_train_user_history_tweets_str+' hashtag past context tweets include: '+pos_past_train_hashtag_context_tweets_str+'  hashtag future context tweets include: '+pos_future_train_hashtag_context_tweets_str
            input_user_hashtag_interact_pair = {'text':input_sentence, 'label':1, 'user_id':global_user_id, 'hashtag':tag}
            input_past_train_set.append(input_user_hashtag_interact_pair)
        
        # establish thisUser-manyHashtag pairs: negative samples of user-hashtag interact pairs
        # sample negative hashtags of ten times number of postive hashtags
        negative_tags = random.sample(list(set(list(past_train_hashtag_aggre_info.keys()))-set(positive_tags)), 10*len(positive_tags))
        for tag in negative_tags:
            neg_past_train_hashtag_context_tweets = []
            for user_id in past_train_hashtag_aggre_info[tag]:
                neg_past_train_tag_context_tweets_from_each_user_list = past_train_hashtag_aggre_info[tag][user_id]
                neg_past_train_hashtag_context_tweets = list(set(neg_past_train_hashtag_context_tweets+neg_past_train_tag_context_tweets_from_each_user_list))
            neg_past_train_hashtag_context_tweets_str = process_sort_tweets_by_time(neg_past_train_hashtag_context_tweets)
            
            neg_future_train_hashtag_context_tweets = []
            for user_id in past_train_hashtag_aggre_info[tag]:
                neg_future_train_tag_context_tweets_from_each_user_list = past_train_hashtag_aggre_info[tag][user_id]
                neg_past_train_hashtag_context_tweets = list(set(neg_future_train_hashtag_context_tweets+neg_future_train_tag_context_tweets_from_each_user_list))
            neg_future_train_hashtag_context_tweets_str = process_sort_tweets_by_time(neg_past_train_hashtag_context_tweets)
            input_sentence = 'user past history tweets include: '+past_train_user_history_tweets_str+' user future history tweets include: '+future_train_user_history_tweets_str+' hashtag past context tweets include: '+neg_past_train_hashtag_context_tweets_str+'  hashtag future context tweets include: '+neg_future_train_hashtag_context_tweets_str
            input_user_hashtag_interact_pair = {'text':input_sentence, 'label':0, 'user_id':global_user_id, 'hashtag':tag}
            input_past_train_set.append(input_user_hashtag_interact_pair)
    
    tempf = open('dataset/input_future_training_data.json', 'w')
    tempf.close()  
    with open('dataset/input_future_training_data.json', 'a') as out:
        for l in input_past_train_set:
            json_str=json.dumps(l)
            out.write(json_str+"\n")   
            
    
def formulate_llm_input_future_test(user_list, past_train_user_aggre_info, past_train_hashtag_aggre_info, future_train_user_aggre_info, future_train_hashtag_aggre_info, future_test_user_aggre_info, future_test_hashtag_aggre_info):
    input_future_test_set = []
    # for each user, establish many user-hashtag pairs
    all_new_tag_count = 0
    all_old_tag_count = 0
    all_past_user_history_tweets_count = 0
    all_test_user_history_tweets_count = 0
    for global_user_id in user_list:
        # input_future_train_set.append('\nuser_id: '+user_id+'\n')
        # acquire this user's history tweets
        user_history_tweets = []
        for tag in past_train_user_aggre_info[global_user_id]:
            all_past_user_history_tweets_count+=len(past_train_user_aggre_info[global_user_id][tag])  
            tweets_list = past_train_user_aggre_info[global_user_id][tag]
            user_history_tweets = list(set(user_history_tweets+tweets_list))
        for tag in future_train_user_aggre_info[global_user_id]:
            tweets_list = future_train_user_aggre_info[global_user_id][tag]
            user_history_tweets = list(set(user_history_tweets+tweets_list))
        user_history_tweets_str = process_sort_tweets_by_time(user_history_tweets)
        
        # establish thisUser-manyHashtag pairs: positive samples of user-hashtag interact pairs
        new_tag_count = 0
        old_tag_count = 0
        positive_tags = []

        for tag in future_test_user_aggre_info[global_user_id].keys():  
            all_test_user_history_tweets_count+=len(future_test_user_aggre_info[global_user_id][tag])  
            positive_tags.append(tag)
            # acquire all hashtag contexts from users who used it
            pos_hashtag_context_tweets = []
            new_tag_flag = 0
            if tag in past_train_hashtag_aggre_info:
                for user_id in past_train_hashtag_aggre_info[tag]:
                    pos_tag_context_tweets_from_each_user_list = past_train_hashtag_aggre_info[tag][user_id]
                    pos_hashtag_context_tweets = list(set(pos_hashtag_context_tweets+pos_tag_context_tweets_from_each_user_list))
            else:
                new_tag_flag+=0.5
            if tag in future_train_hashtag_aggre_info:
                for user_id in future_train_hashtag_aggre_info[tag]:
                    pos_tag_context_tweets_from_each_user_list = future_train_hashtag_aggre_info[tag][user_id]
                    pos_hashtag_context_tweets = list(set(pos_hashtag_context_tweets+pos_tag_context_tweets_from_each_user_list))
            else:
                new_tag_flag+=0.5
            if new_tag_flag==1:
                new_tag_count+=1
            else:
                old_tag_count+=1
            for user_id in future_test_hashtag_aggre_info[tag]:
                # note that the future tweet by the specific user who we want to predict whether would interact with hashtag x, the user's future tweets are inaccessible
                if not (user_id == global_user_id):
                    pos_tag_context_tweets_from_each_user_list = future_test_hashtag_aggre_info[tag][user_id]
                    pos_hashtag_context_tweets = list(set(pos_hashtag_context_tweets+pos_tag_context_tweets_from_each_user_list))
            pos_hashtag_context_tweets_str = process_sort_tweets_by_time(pos_hashtag_context_tweets)
            input_sentence = 'user history tweets include: '+user_history_tweets_str+'hashtag context tweets include: '+pos_hashtag_context_tweets_str
            input_user_hashtag_interact_pair = {'text':input_sentence, 'label':1, 'user_id':global_user_id, 'hashtag':tag}
            input_future_test_set.append(input_user_hashtag_interact_pair)
        
        # establish thisUser-manyHashtag pairs: negative samples of user-hashtag interact pairs
        # sample negative hashtags of 10 times number of postive hashtags, apply
        # negative hashtags = all hashtags - postive hashtags -> do not apply, too much
        negative_tags = random.sample(list(set(list(future_test_hashtag_aggre_info.keys()))-set(positive_tags)), 10*len(positive_tags))
        # negative_tags = list(set(list(future_test_hashtag_aggre_info.keys()))-set(positive_tags))
        for tag in negative_tags:
            neg_hashtag_context_tweets = []
            new_tag_flag = 0
            if tag in past_train_hashtag_aggre_info:
                for user_id in past_train_hashtag_aggre_info[tag]:
                    neg_tag_context_tweets_from_each_user_list = past_train_hashtag_aggre_info[tag][user_id]
                    neg_hashtag_context_tweets = list(set(neg_hashtag_context_tweets+neg_tag_context_tweets_from_each_user_list))
            else:
                pass
                # new_tag_flag+=0.5
            if tag in future_train_hashtag_aggre_info:
                for user_id in future_train_hashtag_aggre_info[tag]:
                    neg_tag_context_tweets_from_each_user_list = future_train_hashtag_aggre_info[tag][user_id]
                    neg_hashtag_context_tweets = list(set(neg_hashtag_context_tweets+neg_tag_context_tweets_from_each_user_list))
            else:
                # new_tag_flag+=0.5
                pass
            # if new_tag_flag==1:
            #     new_tag_count+=1
            # else:
            #     old_tag_count+=1
            for user_id in future_test_hashtag_aggre_info[tag]:
                # note that the future tweet by the specific user who we want to predict whether would interact with hashtag x, the user's future tweets are inaccessible
                if not (user_id == global_user_id):
                    neg_tag_context_tweets_from_each_user_list = future_test_hashtag_aggre_info[tag][user_id]
                    neg_hashtag_context_tweets = list(set(neg_hashtag_context_tweets+neg_tag_context_tweets_from_each_user_list))
            neg_hashtag_context_tweets_str = process_sort_tweets_by_time(neg_hashtag_context_tweets)
            input_sentence = 'user history tweets include: '+user_history_tweets_str+'hashtag context tweets include: '+neg_hashtag_context_tweets_str
            input_user_hashtag_interact_pair = {'text':input_sentence, 'label':0, 'user_id':global_user_id, 'hashtag':tag}
            input_future_test_set.append(input_user_hashtag_interact_pair)
        all_new_tag_count+=new_tag_count
        all_old_tag_count+=old_tag_count
    print('when testing, for all users in user_list, new/old hashtag = '+str(all_new_tag_count)+'/'+str(all_old_tag_count))
    print('totally '+str(all_past_user_history_tweets_count)+' history tweets for all users in past training time')
    print('totally '+str(all_test_user_history_tweets_count)+' history tweets for all users in future testing time')
    
    tempf = open('dataset/input_future_test_data.json', 'w')
    tempf.close()
    with open('dataset/input_future_test_data.json', 'a') as out:
        for l in input_future_test_set:
            json_str=json.dumps(l)
            out.write(json_str+"\n")  


def formulate_llm_input_past_future_test(user_list, past_train_user_aggre_info, past_train_hashtag_aggre_info, future_train_user_aggre_info, future_train_hashtag_aggre_info, future_test_user_aggre_info, future_test_hashtag_aggre_info):
    input_future_test_set = []
    # for each user, establish many user-hashtag pairs
    all_new_tag_count = 0
    all_old_tag_count = 0
    all_past_user_history_tweets_count = 0
    all_test_user_history_tweets_count = 0
    for global_user_id in user_list:
        # input_future_train_set.append('\nuser_id: '+user_id+'\n')
        # acquire this user's history tweets
        user_history_tweets = []
        for tag in past_train_user_aggre_info[global_user_id]:
            all_past_user_history_tweets_count+=len(past_train_user_aggre_info[global_user_id][tag])  
            tweets_list = past_train_user_aggre_info[global_user_id][tag]
            user_history_tweets = list(set(user_history_tweets+tweets_list))
        for tag in future_train_user_aggre_info[global_user_id]:
            tweets_list = future_train_user_aggre_info[global_user_id][tag]
            user_history_tweets = list(set(user_history_tweets+tweets_list))
        user_history_tweets_str = process_sort_tweets_by_time(user_history_tweets)
        
        # establish thisUser-manyHashtag pairs: positive samples of user-hashtag interact pairs
        new_tag_count = 0
        old_tag_count = 0
        positive_tags = []

        for tag in future_test_user_aggre_info[global_user_id].keys():  
            all_test_user_history_tweets_count+=len(future_test_user_aggre_info[global_user_id][tag])  
            positive_tags.append(tag)
            # acquire all hashtag contexts from users who used it
            pos_past_train_hashtag_context_tweets = []
            new_tag_flag = 0
            if tag in past_train_hashtag_aggre_info:
                for user_id in past_train_hashtag_aggre_info[tag]:
                    pos_past_train_tag_context_tweets_from_each_user_list = past_train_hashtag_aggre_info[tag][user_id]
                    pos_past_train_hashtag_context_tweets = list(set(pos_past_train_hashtag_context_tweets+pos_past_train_tag_context_tweets_from_each_user_list))
            else:
                new_tag_flag+=0.5
            if tag in future_train_hashtag_aggre_info:
                for user_id in future_train_hashtag_aggre_info[tag]:
                    pos_past_train_tag_context_tweets_from_each_user_list = future_train_hashtag_aggre_info[tag][user_id]
                    pos_past_train_hashtag_context_tweets = list(set(pos_past_train_hashtag_context_tweets+pos_past_train_tag_context_tweets_from_each_user_list))
            else:
                new_tag_flag+=0.5
            pos_past_train_hashtag_context_tweets_str = process_sort_tweets_by_time(pos_past_train_hashtag_context_tweets)
            if new_tag_flag==1:
                new_tag_count+=1
            else:
                old_tag_count+=1
            pos_future_test_hashtag_context_tweets = []
            for user_id in future_test_hashtag_aggre_info[tag]:
                # note that the future tweet by the specific user who we want to predict whether would interact with hashtag x, the user's future tweets are inaccessible
                if not (user_id == global_user_id):
                    pos_tag_future_test_context_tweets_from_each_user_list = future_test_hashtag_aggre_info[tag][user_id]
                    pos_future_test_hashtag_context_tweets = list(set(pos_future_test_hashtag_context_tweets+pos_tag_future_test_context_tweets_from_each_user_list))
            pos_future_test_hashtag_context_tweets_str = process_sort_tweets_by_time(pos_future_test_hashtag_context_tweets)
            
            input_sentence = 'user history tweets include: '+user_history_tweets_str+' hashtag past context tweets include: '+pos_past_train_hashtag_context_tweets_str+' hashtag future context tweets include: '+pos_future_test_hashtag_context_tweets_str
            input_user_hashtag_interact_pair = {'text':input_sentence, 'label':1, 'user_id':global_user_id, 'hashtag':tag}
            input_future_test_set.append(input_user_hashtag_interact_pair)
        
        # establish thisUser-manyHashtag pairs: negative samples of user-hashtag interact pairs
        # sample negative hashtags of 10 times number of postive hashtags, apply
        # negative hashtags = all hashtags - postive hashtags -> do not apply, too much
        negative_tags = random.sample(list(set(list(future_test_hashtag_aggre_info.keys()))-set(positive_tags)), 10*len(positive_tags))
        # negative_tags = list(set(list(future_test_hashtag_aggre_info.keys()))-set(positive_tags))
        for tag in negative_tags:
            neg_past_train_hashtag_context_tweets = []
            new_tag_flag = 0
            if tag in past_train_hashtag_aggre_info:
                for user_id in past_train_hashtag_aggre_info[tag]:
                    neg_tag_past_train_context_tweets_from_each_user_list = past_train_hashtag_aggre_info[tag][user_id]
                    neg_past_train_hashtag_context_tweets = list(set(neg_past_train_hashtag_context_tweets+neg_tag_past_train_context_tweets_from_each_user_list))
            else:
                pass
                # new_tag_flag+=0.5
            if tag in future_train_hashtag_aggre_info:
                for user_id in future_train_hashtag_aggre_info[tag]:
                    neg_tag_past_train_context_tweets_from_each_user_list = future_train_hashtag_aggre_info[tag][user_id]
                    neg_past_train_hashtag_context_tweets = list(set(neg_past_train_hashtag_context_tweets+neg_tag_past_train_context_tweets_from_each_user_list))
            else:
                # new_tag_flag+=0.5
                pass
            neg_past_train_hashtag_context_tweets_str = process_sort_tweets_by_time(neg_past_train_hashtag_context_tweets)
            # if new_tag_flag==1:
            #     new_tag_count+=1
            # else:
            #     old_tag_count+=1
            
            neg_future_test_hashtag_context_tweets = []
            for user_id in future_test_hashtag_aggre_info[tag]:
                # note that the future tweet by the specific user who we want to predict whether would interact with hashtag x, the user's future tweets are inaccessible
                if not (user_id == global_user_id):
                    neg_tag_context_tweets_from_each_user_list = future_test_hashtag_aggre_info[tag][user_id]
                    neg_future_test_hashtag_context_tweets = list(set(neg_future_test_hashtag_context_tweets+neg_tag_context_tweets_from_each_user_list))
            neg_future_test_hashtag_context_tweets_str = process_sort_tweets_by_time(neg_future_test_hashtag_context_tweets)
            
            input_sentence = 'user history tweets include: '+user_history_tweets_str+' hashtag past context tweets include: '+neg_past_train_hashtag_context_tweets_str+' hashtag future context tweets include: '+neg_future_test_hashtag_context_tweets_str
            input_user_hashtag_interact_pair = {'text':input_sentence, 'label':0, 'user_id':global_user_id, 'hashtag':tag}
            input_future_test_set.append(input_user_hashtag_interact_pair)
        all_new_tag_count+=new_tag_count
        all_old_tag_count+=old_tag_count
        
    print('when testing, for all users in user_list, new/old hashtag = '+str(all_new_tag_count)+'/'+str(all_old_tag_count))
    print('totally '+str(all_past_user_history_tweets_count)+' history tweets for all users in past training time')
    print('totally '+str(all_test_user_history_tweets_count)+' history tweets for all users in future testing time')
    
    tempf = open('dataset/input_past_future_test_data.json', 'w')
    tempf.close()
    with open('dataset/input_past_future_test_data.json', 'a') as out:
        for l in input_future_test_set:
            json_str=json.dumps(l)
            out.write(json_str+"\n")   



def train_valid_test_partition():
    past_train_user_aggre_info = read_json_file_to_dict('past_train_user_aggre_info_dict')    
    past_train_hashtag_aggre_info = read_json_file_to_dict('past_train_hashtag_aggre_info_dict')
    future_train_user_aggre_info = read_json_file_to_dict('future_train_user_aggre_info_dict')
    future_train_hashtag_aggre_info = read_json_file_to_dict('future_train_hashtag_aggre_info_dict')
    future_test_user_aggre_info = read_json_file_to_dict('future_test_user_aggre_info_dict')
    future_test_hashtag_aggre_info = read_json_file_to_dict('future_test_hashtag_aggre_info_dict')
    
    user_list = list(past_train_user_aggre_info.keys())[:50]
    
    # all tweets for user or hashtag are from the past training data
    formulate_llm_input_past_train(user_list, past_train_user_aggre_info, past_train_hashtag_aggre_info)
    
    # in training, differentiate past and future tweets of user or hashtag 
    formulate_llm_input_past_future_train(user_list, past_train_user_aggre_info, past_train_hashtag_aggre_info, future_train_user_aggre_info, future_train_hashtag_aggre_info)
    
    # in testing, do not differentiate past and future tweets for user or hashtag
    formulate_llm_input_future_test(user_list, past_train_user_aggre_info, past_train_hashtag_aggre_info, future_train_user_aggre_info, future_train_hashtag_aggre_info, future_test_user_aggre_info, future_test_hashtag_aggre_info)
    
    # in testing, differentiate past and future tweets for user or hashtag
    formulate_llm_input_past_future_test(user_list, past_train_user_aggre_info, past_train_hashtag_aggre_info, future_train_user_aggre_info, future_train_hashtag_aggre_info, future_test_user_aggre_info, future_test_hashtag_aggre_info)
    
    print('past train hashtag number: '+str(len(past_train_hashtag_aggre_info.keys())))
    print('future train hashtag number: '+str(len(future_train_hashtag_aggre_info.keys())))
    print('future test hashtag number: '+str(len(future_test_hashtag_aggre_info.keys())))
    

def read_all(data_dir):
    regenerate_filtered_user_with_5_more_tweets_per_month_with_hashtags = False
    second_filter_user_with_proper_hashtag_enough_tweets = False
    process_user_hashtag_aggre_partition = False
    
    if regenerate_filtered_user_with_5_more_tweets_per_month_with_hashtags:
        day_dir = os.listdir(data_dir)
        day_dir.sort()
        print('days: '+str(day_dir[:3]))
        
        # month 1, 2, 3, 4, 5, 6
        '''
        for mon in range(1, 7):
            print('-'*50+' processing month of '+str(mon))
            start_day = day_dir.index('20220'+str(mon)+'01')
            end_day = day_dir.index('20220'+str(mon+1)+'01')
            print('start day is: '+str(day_dir[start_day])+', end day is: '+str(day_dir[end_day]))
            for day in day_dir[start_day:end_day]:
                if str(day)[0]=='2' and str(day)[5]==str(mon):
                    each_day_user_tweet_dict = read_day(data_dir+'/'+day)
                    write_dict_to_json_file(each_day_user_tweet_dict, 'pre_filter_user_with_hashtags_day'+str(day))
        '''
        for mon in range(1, 7):  
            print('-'*50+' processing month of '+str(mon)+' for filtering user with enough tweets')  
            start_day = day_dir.index('20220'+str(mon)+'01')
            end_day = day_dir.index('20220'+str(mon+1)+'01')
            
            only_tweet_num_dict = {}
            for day in tqdm(day_dir[start_day:end_day], desc='month_'+str(mon)+', filter user with enough tweets'):
                each_day_user_tweet_dict = read_json_file_to_dict('pre_filter_user_with_hashtags_day'+str(day))
                for user_id in each_day_user_tweet_dict.keys():
                    try:
                        only_tweet_num_dict[user_id]['user_tweet_num'] += each_day_user_tweet_dict[user_id]['user_tweet_num']
                    except:
                        only_tweet_num_dict[user_id] = {}
                        only_tweet_num_dict[user_id]['user_tweet_num'] = each_day_user_tweet_dict[user_id]['user_tweet_num']
                        
            # show the filtered users who have enough history tweets
            filtered_user_tweet_empty_dict = {}
            filtered_user_tweet_empty_dict = filter_user_with_enough_tweets(only_tweet_num_dict, filtered_user_tweet_empty_dict)
            
            write_dict_to_json_file(filtered_user_tweet_empty_dict, 'empty_first_filter_user_with_enough_tweets'+str(mon))
            
            
            filtered_user_tweet_empty_dict = read_json_file_to_dict('empty_first_filter_user_with_enough_tweets'+str(mon))
            
            filtered_user_tweet_full_dict = {}
            for day in tqdm(day_dir[start_day:end_day], desc='For each day, inject tweet entities into the filtered_user_tweet_full_dict'):
                each_day_user_tweet_dict = read_json_file_to_dict('pre_filter_user_with_hashtags_day'+str(day))
                slim_each_day_user_tweet_dict = slim_tweet_form(each_day_user_tweet_dict)
                for user_id in slim_each_day_user_tweet_dict.keys():
                    if user_id in filtered_user_tweet_empty_dict.keys():
                        for each_tweet in slim_each_day_user_tweet_dict[user_id]['tweet_entities']:
                            if user_id in filtered_user_tweet_full_dict:
                                if each_tweet not in filtered_user_tweet_full_dict[user_id]['tweet_entities']:
                                    filtered_user_tweet_full_dict[user_id]['tweet_entities'].append(each_tweet)
                            else:
                                filtered_user_tweet_full_dict[user_id] = {}
                                filtered_user_tweet_full_dict[user_id]['tweet_entities'] = slim_each_day_user_tweet_dict[user_id]['tweet_entities']
                    
            write_dict_to_json_file(filtered_user_tweet_full_dict, 'first_filter_user_enough_tweets_mon'+str(mon))
            
    elif second_filter_user_with_proper_hashtag_enough_tweets:
        for mon in range(1, 7):
            # show the filtered users with propor hashtags with enough contexts
            filtered_user_tweet_dict = read_json_file_to_dict('first_filter_user_enough_tweets_mon'+str(mon))
            filter_user_hashtag_tweet_dict = filter_user_with_proper_hashtags(filtered_user_tweet_dict)
            
            write_dict_to_json_file(filter_user_hashtag_tweet_dict, 'second_filter_user_proper_tag_enough_tweets_mon'+str(mon))
    elif process_user_hashtag_aggre_partition:
        user_hashtag_aggre_partition()
        
        
    else:   
        train_valid_test_partition()


if __name__ =='__main__':
    
    data_dir='./2022_twitter_data'
    
    read_all(data_dir=data_dir)
