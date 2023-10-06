import os
import json
import gzip
from tqdm import tqdm
import copy


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
                if sec['entities']['hashtags'] and not(sec['retweeted']):
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
        if user_tweet_dict[user_id]['user_tweet_num']>=8 and user_tweet_dict[user_id]['user_tweet_num']<=50: # 1, 2: 5-na, 3, 4: 6-100, 5: 60, 6:8-50
            filtered_user_tweet_dict[user_id]=user_tweet_dict[user_id]
            num_filtered_users_tweets+=filtered_user_tweet_dict[user_id]['user_tweet_num']
            # num_filtered_user_tweet_dict[user_id]=user_tweet_dict[user_id]['user_tweet_num']

    # print(num_filtered_user_tweet_dict) 
    print('There are totally '+str(len(filtered_user_tweet_dict))+' users who have 5 more tweets in one month, with totally '+str(num_filtered_users_tweets)+' tweets')     
    print('The original number of users in these days is: '+str(len(user_tweet_dict)))
    
    return filtered_user_tweet_dict

def filter_user_with_proper_hashtags(filtered_user_tweet_dict):
    hashtag_num_dict = {}
    new_hashtag_user_dict = {}
    new_hashtag_list=[]
    for user_id in tqdm(filtered_user_tweet_dict.keys(), desc='constructing the hashtag_num_dict '):
        for each_tweet in filtered_user_tweet_dict[user_id]['tweet_entities']:
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
            hashtags = copy.deepcopy(each_tweet['entities']['hashtags']) # only copy the value without the handle
            new_proper_hashtags = []
            for hashtag in hashtags:
                hashtag['text'] = hashtag['text'].lower()
                if hashtag_num_dict[hashtag['text']]>=20 and len(hashtag['text'])>=5 and hashtag_num_dict[hashtag['text']]<=500 and len(new_hashtag_user_dict[hashtag['text']])>=5 and len(new_hashtag_user_dict[hashtag['text']]) <= 500:
                    new_hashtag_list.append(hashtag['text'])
                    new_proper_hashtags.append(hashtag)
                    each_tweet['entities']['hashtags']=new_proper_hashtags
                    if each_tweet not in new_history_tweets:
                        new_history_tweets.append(each_tweet)
                    
        if len(new_history_tweets) >= 10 and len(new_history_tweets) <= 60:
            filter_user_hashtag_tweet_dict[user_id]=new_history_tweets
                 
    print('the total number of filtered users with proper hashtags and 10 more history tweets in one month is: '+str(len(filter_user_hashtag_tweet_dict)))
    print('the total number of the filtered hashtags is: '+str(len(list(set(new_hashtag_list)))))
    return filter_user_hashtag_tweet_dict


def slim_tweet_form(each_day_user_tweet_dict):
    slim_each_day_user_tweet_dict = {}
    for user_id in each_day_user_tweet_dict:
        slim_each_day_user_tweet_dict[user_id]={'tweet_entities':[]}
        for each_tweet in each_day_user_tweet_dict[user_id]['tweet_entities']:
            each_slim_tweet = {key:val for key, val in each_tweet.items() if (key=='created_at' or key=='id_str' or key=='text' or key=='source' or key=='entities')}
            slim_each_day_user_tweet_dict[user_id]['tweet_entities'].append(each_slim_tweet)
    
    return slim_each_day_user_tweet_dict


def write_dict_to_json_file(filtered_user_tweet_dict, mon):
    filter_user_dict_obj = json.dumps(filtered_user_tweet_dict)
    filter_user_file = open('middle_processed_data/'+str(mon)+'.json', 'w')
    filter_user_file.write(filter_user_dict_obj)
    filter_user_file.close()
    
    
def read_json_file_to_dict(mon):
    filter_user_file = open('middle_processed_data/'+str(mon)+'.json', 'r')
    filter_user_dict_obj = filter_user_file.read()
    filtered_user_tweet_dict = json.loads(filter_user_dict_obj)
    filter_user_file.close()
    return filtered_user_tweet_dict


def uniform_timestamp(ori_time):
    uni_time = ori_time
    return uni_time


def train_valid_test_partition():
    all_tweets_first = []
    # mon 1, 2, 3, 4, 5, 6
    for mon in range(1, 7):
        filter_user_hashtag_tweet_dict = read_json_file_to_dict('second_filter_user_proper_tag_enough_tweets_mon'+str(mon))
        for user in tqdm(filter_user_hashtag_tweet_dict.keys()):
            for each_tweet in filter_user_hashtag_tweet_dict[user]:
                tweet_id = each_tweet['id_str']
                hashtag_list = each_tweet['entities']['hashtags']
                user_id = user
                timestamp = uniform_timestamp(each_tweet['created_at'])
            



def read_all(data_dir):
    
    regenerate_filtered_user_with_5_more_tweets_per_month_with_hashtags = False
    second_filter_user_with_proper_hashtag_enough_tweets = False
    
    
    if regenerate_filtered_user_with_5_more_tweets_per_month_with_hashtags:
        day_dir = os.listdir(data_dir)
        day_dir.sort()
        print('days: '+str(day_dir[:3]))
        
        
        # month 1, 2, 3, 4, 5, 6
        for mon in range(1, 7):
            print('-'*50+' processing month of '+str(mon))
            start_day = day_dir.index('20220'+str(mon)+'01')
            end_day = day_dir.index('20220'+str(mon+1)+'01')
            print('start day is: '+str(day_dir[start_day])+', end day is: '+str(day_dir[end_day]))
            for day in day_dir[start_day:end_day]:
                if str(day)[0]=='2' and str(day)[5]==str(mon):
                    each_day_user_tweet_dict = read_day(data_dir+'/'+day)
                    write_dict_to_json_file(each_day_user_tweet_dict, 'pre_filter_user_with_hashtags_day'+str(day))
        
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
    else:
        train_valid_test_partition()
        
  
  
if __name__ =='__main__':
    
    data_dir='./2022_twitter_data'
    
    read_all(data_dir=data_dir)