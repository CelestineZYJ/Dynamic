import random
import json
import string
import re


def analyze_messages(tweets_list):
    sorted_tweets = ''
    tweets_list = list(set(tweets_list))
    if len(tweets_list)>20:
        tweets_list = random.sample(tweets_list, 20)
    # tweets_list.sort()
    for tweet in tweets_list:
        sorted_tweets+=(tweet+'\n')
    return sorted_tweets

def filter_meaningful_tweet(tweet_str):
    filtered_tweet = ''
    new_str = re.sub(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*', '', tweet_str).lower()
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
    if len(new_str.split(' '))-new_str.count('@')>=5:
        filtered_tweet = new_str.replace('@', '')
    else:
        filtered_tweet = False
    return filtered_tweet


def read_all(data_dir, topic_category):
    mon1file = open(data_dir+topic_category+'_201501.data', 'r')
    mon1lines = mon1file.readlines()
    mon2file = open(data_dir+topic_category+'_201502.data', 'r')
    mon2lines = mon2file.readlines()
    mon3file = open(data_dir+topic_category+'_201503.data', 'r')
    mon3lines = mon3file.readlines()
    mon4file = open(data_dir+topic_category+'_201504.data', 'r')
    mon4lines = mon4file.readlines()
    testmon5file = open(data_dir+topic_category+'_201505_test.data', 'r')
    testmon5lines = testmon5file.readlines()

    ##################################################################################past_train+future_train_future_test:conv_message_dict####################################
    past_train_conv_messages = {}
    for mon1line in mon1lines:
        mon1line = mon1line.strip('\n').split('\t')
        if len(mon1line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon1line[0], mon1line[1], mon1line[2], mon1line[3], mon1line[4], mon1line[5], mon1line[6], mon1line[7], mon1line[8]
            # each message sentence must have more than 5 words
            proce_sen = filter_meaningful_tweet(proce_sen)
            if proce_sen: 
                if conv_id not in past_train_conv_messages:
                    past_train_conv_messages[conv_id] = {'messas':[{'messa_id':messa_id, 'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon01'}], 'messa_num':1}
                    # past_train_conv_messages[conv_id] = {messa_id:{'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon01'}, 'messa_num':1}
                else: 
                    past_train_conv_messages[conv_id]['messa_num']+=1
                    past_train_conv_messages[conv_id]['messas'].append({'messa_id':messa_id, 'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon01'})
    for mon2line in mon2lines:
        mon2line = mon2line.strip('\n').split('\t')
        if len(mon2line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon2line[0], mon2line[1], mon2line[2], mon2line[3], mon2line[4], mon2line[5], mon2line[6], mon2line[7], mon2line[8]
            # each message sentence must have more than 5 words
            proce_sen = filter_meaningful_tweet(proce_sen)
            if proce_sen: 
                if conv_id not in past_train_conv_messages:
                    past_train_conv_messages[conv_id] = {'messas':[{'messa_id':messa_id, 'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon02'}], 'messa_num':1}
                else:
                    past_train_conv_messages[conv_id]['messa_num']+=1
                    past_train_conv_messages[conv_id]['messas'].append({'messa_id':messa_id, 'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon02'})
    for mon3line in mon3lines:
        mon3line = mon3line.strip('\n').split('\t')
        if len(mon3line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon3line[0], mon3line[1], mon3line[2], mon3line[3], mon3line[4], mon3line[5], mon3line[6], mon3line[7], mon3line[8]
            # each message sentence must have more than 5 words
            proce_sen = filter_meaningful_tweet(proce_sen)
            if proce_sen: 
                if conv_id not in past_train_conv_messages:
                    past_train_conv_messages[conv_id] = {'messas':[{'messa_id':messa_id, 'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon03'}], 'messa_num':1}
                else: 
                    past_train_conv_messages[conv_id]['messa_num']+=1
                    past_train_conv_messages[conv_id]['messas'].append({'messa_id':messa_id, 'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon03'})

    future_train_conv_messages = {}
    for mon4line in mon4lines:
        mon4line = mon4line.strip('\n').split('\t')
        if len(mon4line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon4line[0], mon4line[1], mon4line[2], mon4line[3], mon4line[4], mon4line[5], mon4line[6], mon4line[7], mon4line[8]
            # each message sentence must have more than 5 words
            proce_sen = filter_meaningful_tweet(proce_sen)
            if proce_sen: 
                if conv_id not in future_train_conv_messages:
                    future_train_conv_messages[conv_id] = {'messas':[{'messa_id':messa_id, 'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon04'}], 'messa_num':1}
                else: 
                    future_train_conv_messages[conv_id]['messa_num']+=1
                    future_train_conv_messages[conv_id]['messas'].append({'messa_id':messa_id, 'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon04'})
    
    future_test_conv_messages = {}
    for mon5line in testmon5lines:
        mon5line = mon5line.strip('\n').split('\t')
        if len(mon5line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon5line[0], mon5line[1], mon5line[2], mon5line[3], mon5line[4], mon5line[5], mon5line[6], mon5line[7], mon5line[8]
            # each message sentence must have more than 5 words
            proce_sen = filter_meaningful_tweet(proce_sen)
            if proce_sen: 
                if conv_id not in future_test_conv_messages:
                    future_test_conv_messages[conv_id] = {'messas':[{'messa_id':messa_id, 'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon05'}], 'messa_num':1}
                else: 
                    future_test_conv_messages[conv_id]['messa_num']+=1
                    future_test_conv_messages[conv_id]['messas'].append({'messa_id':messa_id, 'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon05'})

    ##################################################################################past_train+future_train_future_test:user_message_dict####################################
    past_train_user_messages = {}
    for mon1line in mon1lines:
        mon1line = mon1line.strip('\n').split('\t')
        if len(mon1line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon1line[0], mon1line[1], mon1line[2], mon1line[3], mon1line[4], mon1line[5], mon1line[6], mon1line[7], mon1line[8]
            # each message sentence must have more than 5 words
            proce_sen = filter_meaningful_tweet(proce_sen)
            if proce_sen: 
                if uid not in past_train_user_messages:
                    past_train_user_messages[uid] = {'convs':[{'conv_id':conv_id, 'messa_text':str(proce_sen), 'messa_id':str(messa_id), 'time':'mon01'}], 'messa_num':1}
                else: 
                    past_train_user_messages[uid]['messa_num']+=1
                    past_train_user_messages[uid]['convs'].append({'conv_id':conv_id, 'messa_text':str(proce_sen), 'messa_id':str(messa_id), 'time':'mon01'})
    for mon2line in mon2lines:
        mon2line = mon2line.strip('\n').split('\t')
        if len(mon2line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon2line[0], mon2line[1], mon2line[2], mon2line[3], mon2line[4], mon2line[5], mon2line[6], mon2line[7], mon2line[8]
            # each message sentence must have more than 5 words
            proce_sen = filter_meaningful_tweet(proce_sen)
            if proce_sen: 
                if uid not in past_train_user_messages:
                    past_train_user_messages[uid] = {'convs':[{'conv_id':conv_id, 'messa_text':str(proce_sen), 'messa_id':str(messa_id), 'time':'mon02'}], 'messa_num':1}
                else: 
                    past_train_user_messages[uid]['messa_num']+=1
                    past_train_user_messages[uid]['convs'].append({'conv_id':conv_id, 'messa_text':str(proce_sen), 'messa_id':str(messa_id), 'time':'mon02'})
    for mon3line in mon3lines:
        mon3line = mon3line.strip('\n').split('\t')
        if len(mon3line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon3line[0], mon3line[1], mon3line[2], mon3line[3], mon3line[4], mon3line[5], mon3line[6], mon3line[7], mon3line[8]
            # each message sentence must have more than 5 words
            proce_sen = filter_meaningful_tweet(proce_sen)
            if proce_sen: 
                if uid not in past_train_user_messages:
                    past_train_user_messages[uid] = {'convs':[{'conv_id':conv_id, 'messa_text':str(proce_sen), 'messa_id':str(messa_id), 'time':'mon03'}], 'messa_num':1}
                else: 
                    past_train_user_messages[uid]['messa_num']+=1
                    past_train_user_messages[uid]['convs'].append({'conv_id':conv_id, 'messa_text':str(proce_sen), 'messa_id':str(messa_id), 'time':'mon03'})
    future_train_user_messages = {}
    for mon4line in mon4lines:
        mon4line = mon4line.strip('\n').split('\t')
        if len(mon4line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon4line[0], mon4line[1], mon4line[2], mon4line[3], mon4line[4], mon4line[5], mon4line[6], mon4line[7], mon4line[8]
            # each message sentence must have more than 5 words
            proce_sen = filter_meaningful_tweet(proce_sen)
            if proce_sen: 
                if uid not in future_train_user_messages:
                    future_train_user_messages[uid] = {'convs':[{'conv_id':conv_id, 'messa_text':str(proce_sen), 'messa_id':str(messa_id), 'time':'mon04'}], 'messa_num':1}
                else: 
                    future_train_user_messages[uid]['messa_num']+=1
                    future_train_user_messages[uid]['convs'].append({'conv_id':conv_id, 'messa_text':str(proce_sen), 'messa_id':str(messa_id), 'time':'mon04'})
    future_test_user_messages = {}
    for mon5line in testmon5lines:
        mon5line = mon5line.strip('\n').split('\t')
        if len(mon5line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon5line[0], mon5line[1], mon5line[2], mon5line[3], mon5line[4], mon5line[5], mon5line[6], mon5line[7], mon5line[8]
            # each message sentence must have more than 5 words
            proce_sen = filter_meaningful_tweet(proce_sen)
            if proce_sen: 
                if uid not in future_test_user_messages:
                    future_test_user_messages[uid] = {'convs':[{'conv_id':conv_id, 'messa_text':str(proce_sen), 'messa_id':str(messa_id), 'time':'mon05'}], 'messa_num':1}
                else: 
                    future_test_user_messages[uid]['messa_num']+=1
                    future_test_user_messages[uid]['convs'].append({'conv_id':conv_id, 'messa_text':str(proce_sen), 'messa_id':str(messa_id), 'time':'mon05'})
    ##########################################################################keep the user list with more than 2 messages####################################
    future_test_user_conv_interact_list = {}
    for mon5line in testmon5lines:
        mon5line = mon5line.strip('\n').split('\t')
        if len(mon5line) == 2:
            conv_id, uid = mon5line[0], mon5line[1]
            # each message sentence must have more than 5 words
            future_test_user_conv_interact_list[uid] = conv_id
    
    new_past_train_user_list = []
    new_future_train_user_list = []
    new_future_test_user_list = []
    print('before deleting the users with less than two messages')
    print(len(past_train_user_messages.keys()))
    print(len(future_train_user_messages.keys()))
    print(len(future_test_user_messages.keys()))
    print(len(future_test_user_conv_interact_list.keys()))
    for uid in future_test_user_conv_interact_list.keys():
        if uid in past_train_user_messages and uid in past_train_user_messages:
            if past_train_user_messages[uid]['messa_num'] >=2:
                new_past_train_user_list.append(uid)
    for uid in future_test_user_conv_interact_list.keys():
        if uid in future_train_user_messages and uid in future_train_user_messages:
            if future_train_user_messages[uid]['messa_num'] >=2:
                new_future_train_user_list.append(uid)  
    for uid in future_test_user_conv_interact_list.keys():
        if uid in future_test_user_messages and uid in future_test_user_messages:
            if future_test_user_messages[uid]['messa_num'] >=2:
                new_future_test_user_list.append(uid)    
    unify_user_conv_interact_list = {}
    for uid in future_test_user_conv_interact_list.keys():
        if uid in new_past_train_user_list and uid in new_future_train_user_list and uid in new_future_test_user_list:
            unify_user_conv_interact_list[uid] = future_test_user_conv_interact_list[uid] # uid: conv_id
    print('\n'+'after deleting the users with less than two messages')
    print(len(new_past_train_user_list))
    print(len(new_future_train_user_list))
    print(len(new_future_test_user_list))
    print(len(unify_user_conv_interact_list.keys()))


    ######################################################################construct the user_conv_interaction_training/test_data####################################

    # formulate the past training data
    input_past_train_set = []
    for uid in unify_user_conv_interact_list.keys():
        user_history_messa = []
        for each_conv in past_train_user_messages[uid]['convs']:
            user_history_messa.append(each_conv['messa_text'])
        num_user_history_messas = past_train_user_messages[uid]['messa_num']
        user_history_messa_str = analyze_messages(user_history_messa)

        pos_conv = []
        for each_conv in past_train_user_messages[uid]['convs']:
            if each_conv['conv_id'] in pos_conv:
                continue
            else:
                if past_train_conv_messages[each_conv['conv_id']]['messa_num'] >=3:
                    num_pos_conv_messas = past_train_conv_messages[each_conv['conv_id']]['messa_num']
                    pos_conv.append(each_conv['conv_id'])
                    pos_conv_context_messas = []
                    for each_messa in past_train_conv_messages[each_conv['conv_id']]['messas']:
                        pos_conv_context_messas.append(each_messa['messa_text'])
                    pos_conv_context_messas_str = analyze_messages(pos_conv_context_messas)
                    input_sentence = 'user history messages include: '+user_history_messa_str+'conversation context messages include: '+pos_conv_context_messas_str
                    input_user_conv_interact_pair = {'text':input_sentence, 'label':1, 'reward_score':0, 'time':0,'user_id':uid, 'conversation':each_conv['conv_id'], 'category':'past_training', 'user_past_history_num': num_user_history_messas, 'user_future_history_num':0, 'pos_conv_past_messa_num':num_pos_conv_messas, 'pos_conv_future_messa_num':0}
                    input_past_train_set.append(input_user_conv_interact_pair)
                else:
                    continue
        
        negative_conv = random.sample(list(set(list(past_train_conv_messages.keys()))-set(pos_conv)), 10*len(pos_conv))
        for conv_id in negative_conv:
            neg_conv_context_messas = []
            if past_train_conv_messages[conv_id]['messa_num'] >=3:
                num_neg_conv_messas = past_train_conv_messages[conv_id]['messa_num']
                for each_messa in past_train_conv_messages[conv_id]['messas']:
                    neg_conv_context_messas.append(each_messa['messa_text'])
                neg_conv_context_messas_str = analyze_messages(neg_conv_context_messas)
                input_sentence = 'user history messages include: '+user_history_messa_str+'conversation context messages include: '+neg_conv_context_messas_str
                input_user_conv_interact_pair = {'text':input_sentence, 'label':0, 'reward_score':0, 'time':0,'user_id':uid, 'conversation':conv_id, 'category':'past_training', 'user_past_history_num': num_user_history_messas, 'user_future_history_num':0, 'neg_conv_past_messa_num':num_neg_conv_messas, 'pos_conv_future_messa_num':0}
                input_past_train_set.append(input_user_conv_interact_pair)
            else:
                continue

    tempf = open('conv_data/input_past_training_data.json', 'w')
    tempf.close()  
    with open('conv_data/input_past_training_data.json', 'a') as out:
        for l in input_past_train_set:
            json_str=json.dumps(l)
            out.write(json_str+"\n")   

    # formulate the future training data
    input_past_future_train_set = []
    for uid in unify_user_conv_interact_list.keys():
        past_user_history_messa = []
        for each_conv in past_train_user_messages[uid]['convs']:
            past_user_history_messa.append(each_conv['messa_text'])
        num_past_user_history_messas = past_train_user_messages[uid]['messa_num']
        past_user_history_messa_str = analyze_messages(past_user_history_messa)
        future_user_history_messa = []
        for each_conv in future_train_user_messages[uid]['convs']:
            future_user_history_messa.append(each_conv['messa_text'])
        num_future_user_history_messas = future_train_user_messages[uid]['messa_num']
        future_user_history_messa_str = analyze_messages(future_user_history_messa)

        pos_conv = []
        for each_conv in future_train_user_messages[uid]['convs']:
            if each_conv['conv_id'] in pos_conv:
                continue
            # for this conversation in future training dict, there exist messages in past training dict
            else:
                pos_past_conv_context_messas_str = ''
                num_pos_past_conv_messas = 0
                if each_conv['conv_id'] in past_train_conv_messages and past_train_conv_messages[each_conv['conv_id']]['messa_num'] >=3:
                    print('positive tags: for this conversation in future training dict, there exist '+str(past_train_conv_messages[each_conv['conv_id']]['messa_num'])+' messages in past training dict')
                    print('conv_id is '+each_conv['conv_id'])
                    num_pos_past_conv_messas = past_train_conv_messages[each_conv['conv_id']]['messa_num']
                    pos_past_conv_context_messas = []
                    for each_messa in past_train_conv_messages[each_conv['conv_id']]['messas']:
                        pos_past_conv_context_messas.append(each_messa['messa_text'])
                    pos_past_conv_context_messas_str = analyze_messages(pos_past_conv_context_messas)

                # for this conversation in future training dict, there is no messages in past training dict
                num_pos_future_conv_messas = future_train_conv_messages[each_conv['conv_id']]['messa_num']
                pos_conv.append(each_conv['conv_id'])
                pos_future_conv_context_messas = []
                for each_messa in future_train_conv_messages[each_conv['conv_id']]['messas']:
                    pos_future_conv_context_messas.append(each_messa['messa_text'])
                pos_future_conv_context_messas_str = analyze_messages(pos_future_conv_context_messas)

                input_sentence = 'user past history messages include: '+past_user_history_messa_str+' user future history messages include: '+future_user_history_messa_str+' conversation past context messages include: '+pos_past_conv_context_messas_str+'  conversation future context messages include: '+pos_future_conv_context_messas_str
                input_user_conv_interact_pair = {'text':input_sentence, 'label':1, 'reward_score':1, 'time':1,'user_id':uid, 'conversation':each_conv['conv_id'], 'category':'future_training_with_past_future_user_conv_messas','user_past_history_num': num_past_user_history_messas, 'user_future_history_num': num_future_user_history_messas, 'pos_conv_past_messa_num':num_pos_past_conv_messas, 'pos_conv_future_messa_num':num_pos_future_conv_messas}
                input_past_future_train_set.append(input_user_conv_interact_pair)
        
        negative_conv = random.sample(list(set(list(future_train_conv_messages.keys()))-set(pos_conv)), 10*len(pos_conv))
        for conv_id in negative_conv:
            neg_past_conv_context_messas = []
            neg_past_conv_context_messas_str = ''
            num_neg_past_conv_messas = 0
            if each_conv['conv_id'] in past_train_conv_messages and past_train_conv_messages[conv_id]['messa_num'] >=3:
                print('negative tags: for this conversation in future training dict, there exist messages in past training dict')
                num_neg_past_conv_messas = past_train_conv_messages[conv_id]['messa_num']
                for each_messa in past_train_conv_messages[conv_id]['messas']:
                    neg_past_conv_context_messas.append(each_messa['messa_text'])
                neg_past_conv_context_messas_str = analyze_messages(neg_past_conv_context_messas)
 
            num_neg_future_conv_messas = future_train_conv_messages[conv_id]['messa_num']
            neg_future_conv_context_messas = []
            for each_messa in future_train_conv_messages[conv_id]['messas']:
                neg_future_conv_context_messas.append(each_messa['messa_text'])
            neg_future_conv_context_messas_str = analyze_messages(neg_future_conv_context_messas)
            input_sentence = 'user past history messages include: '+past_user_history_messa_str+' user future history messages include: '+future_user_history_messa_str+' conversation past context messages include: '+neg_past_conv_context_messas_str+'  conversation future context messages include: '+neg_future_conv_context_messas_str
            input_user_conv_interact_pair = {'text':input_sentence, 'label':0, 'reward_score':1, 'time':1,'user_id':uid, 'conversation':each_conv['conv_id'], 'category':'future_training_with_past_future_user_conv_messas','user_past_history_num': num_past_user_history_messas, 'user_future_history_num': num_future_user_history_messas, 'neg_conv_past_messa_num':num_neg_past_conv_messas, 'neg_conv_future_messa_num':num_neg_future_conv_messas}
            input_past_future_train_set.append(input_user_conv_interact_pair)
    
    tempf = open('conv_data/input_future_training_data.json', 'w')
    tempf.close()  
    with open('conv_data/input_future_training_data.json', 'a') as out:
        for l in input_past_future_train_set:
            json_str=json.dumps(l)
            out.write(json_str+"\n")   
    
    # formulate the future test data
    # later cold-start users that apprear at the future test data only could be tested at the inference stage. For training, we adopt data of users existing through the timeline
    input_future_test_set = []
    for uid in unify_user_conv_interact_list.keys():
        past_user_history_messa = []
        for each_conv in past_train_user_messages[uid]['convs']:
            past_user_history_messa.append(each_conv['messa_text'])
        num_past_user_history_messas = past_train_user_messages[uid]['messa_num']
        past_user_history_messa_str = analyze_messages(past_user_history_messa)
        future_user_history_messa = []
        for each_conv in future_train_user_messages[uid]['convs']:
            future_user_history_messa.append(each_conv['messa_text'])
        num_future_user_history_messas = future_train_user_messages[uid]['messa_num']
        future_user_history_messa_str = analyze_messages(future_user_history_messa)

        pos_conv = []
        for each_conv in future_train_user_messages[uid]['convs']:
            if each_conv['conv_id'] in pos_conv:
                continue
            # for this conversation in future training dict, there exist messages in past training dict
            else:
                pos_past_conv_context_messas_str = ''
                num_pos_past_conv_messas = 0
                if each_conv['conv_id'] in past_train_conv_messages and past_train_conv_messages[each_conv['conv_id']]['messa_num'] >=3:
                    print('positive tags: for this conversation in future training dict, there exist '+str(past_train_conv_messages[each_conv['conv_id']]['messa_num'])+' messages in past training dict')
                    print('conv_id is '+each_conv['conv_id'])
                    num_pos_past_conv_messas = past_train_conv_messages[each_conv['conv_id']]['messa_num']
                    pos_past_conv_context_messas = []
                    for each_messa in past_train_conv_messages[each_conv['conv_id']]['messas']:
                        pos_past_conv_context_messas.append(each_messa['messa_text'])
                    pos_past_conv_context_messas_str = analyze_messages(pos_past_conv_context_messas)

                # for this conversation in future training dict, there is no messages in past training dict
                num_pos_future_conv_messas = future_train_conv_messages[each_conv['conv_id']]['messa_num']
                pos_conv.append(each_conv['conv_id'])
                pos_future_conv_context_messas = []
                for each_messa in future_train_conv_messages[each_conv['conv_id']]['messas']:
                    pos_future_conv_context_messas.append(each_messa['messa_text'])
                pos_future_conv_context_messas_str = analyze_messages(pos_future_conv_context_messas)

                input_sentence = 'user past history messages include: '+past_user_history_messa_str+' user future history messages include: '+future_user_history_messa_str+' conversation past context messages include: '+pos_past_conv_context_messas_str+'  conversation future context messages include: '+pos_future_conv_context_messas_str
                input_user_conv_interact_pair = {'text':input_sentence, 'label':1, 'reward_score':1, 'time':1,'user_id':uid, 'conversation':each_conv['conv_id'], 'category':'future_training_with_past_future_user_conv_messas','user_past_history_num': num_past_user_history_messas, 'user_future_history_num': num_future_user_history_messas, 'pos_conv_past_messa_num':num_pos_past_conv_messas, 'pos_conv_future_messa_num':num_pos_future_conv_messas}
                input_future_test_set.append(input_user_conv_interact_pair)
        
        negative_conv = random.sample(list(set(list(future_train_conv_messages.keys()))-set(pos_conv)), 10*len(pos_conv))
        for conv_id in negative_conv:
            neg_past_conv_context_messas = []
            neg_past_conv_context_messas_str = ''
            num_neg_past_conv_messas = 0
            if each_conv['conv_id'] in past_train_conv_messages and past_train_conv_messages[conv_id]['messa_num'] >=3:
                print('negative tags: for this conversation in future training dict, there exist messages in past training dict')
                num_neg_past_conv_messas = past_train_conv_messages[conv_id]['messa_num']
                for each_messa in past_train_conv_messages[conv_id]['messas']:
                    neg_past_conv_context_messas.append(each_messa['messa_text'])
                neg_past_conv_context_messas_str = analyze_messages(neg_past_conv_context_messas)
 
            num_neg_future_conv_messas = future_train_conv_messages[conv_id]['messa_num']
            neg_future_conv_context_messas = []
            for each_messa in future_train_conv_messages[conv_id]['messas']:
                neg_future_conv_context_messas.append(each_messa['messa_text'])
            neg_future_conv_context_messas_str = analyze_messages(neg_future_conv_context_messas)
            input_sentence = 'user past history messages include: '+past_user_history_messa_str+' user future history messages include: '+future_user_history_messa_str+' conversation past context messages include: '+neg_past_conv_context_messas_str+'  conversation future context messages include: '+neg_future_conv_context_messas_str
            input_user_conv_interact_pair = {'text':input_sentence, 'label':0, 'reward_score':1, 'time':1,'user_id':uid, 'conversation':each_conv['conv_id'], 'category':'future_training_with_past_future_user_conv_messas','user_past_history_num': num_past_user_history_messas, 'user_future_history_num': num_future_user_history_messas, 'neg_conv_past_messa_num':num_neg_past_conv_messas, 'neg_conv_future_messa_num':num_neg_future_conv_messas}
            input_future_test_set.append(input_user_conv_interact_pair)
    
    tempf = open('conv_data/input_future_test_data.json', 'w')
    tempf.close()  
    with open('conv_data/input_future_test_data.json', 'a') as out:
        for l in input_future_test_set:
            json_str=json.dumps(l)
            out.write(json_str+"\n")   
    



if __name__ =='__main__':
    
    data_dir='./online_conv_data/'
    
    read_all(data_dir=data_dir, topic_category='funny')

