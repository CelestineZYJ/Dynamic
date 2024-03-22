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
            if len(proce_sen.split(' '))>=5:
                if conv_id not in past_train_conv_messages:
                    past_train_conv_messages[conv_id] = {messa_id:{'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon01'}, 'messa_num':1}
                else: 
                    past_train_conv_messages[conv_id]['messa_num']+=1
                    past_train_conv_messages[conv_id][messa_id] = {'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon01'}
    for mon2line in mon2lines:
        mon2line = mon2line.strip('\n').split('\t')
        if len(mon2line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon2line[0], mon2line[1], mon2line[2], mon2line[3], mon2line[4], mon2line[5], mon2line[6], mon2line[7], mon2line[8]
            # each message sentence must have more than 5 words
            if len(proce_sen.split(' '))>=5:
                if conv_id not in past_train_conv_messages:
                    past_train_conv_messages[conv_id] = {messa_id:{'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon02'}, 'messa_num':1}
                else: 
                    past_train_conv_messages[conv_id]['messa_num']+=1
                    past_train_conv_messages[conv_id][messa_id] = {'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon02'}
    for mon3line in mon3lines:
        mon3line = mon3line.strip('\n').split('\t')
        if len(mon3line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon3line[0], mon3line[1], mon3line[2], mon3line[3], mon3line[4], mon3line[5], mon3line[6], mon3line[7], mon3line[8]
            # each message sentence must have more than 5 words
            if len(proce_sen.split(' '))>=5:
                if conv_id not in past_train_conv_messages:
                    past_train_conv_messages[conv_id] = {messa_id:{'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon03'}, 'messa_num':1}
                else: 
                    past_train_conv_messages[conv_id]['messa_num']+=1
                    past_train_conv_messages[conv_id][messa_id] = {'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon03'}

    future_train_conv_messages = {}
    for mon4line in mon4lines:
        mon4line = mon4line.strip('\n').split('\t')
        if len(mon4line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon4line[0], mon4line[1], mon4line[2], mon4line[3], mon4line[4], mon4line[5], mon4line[6], mon4line[7], mon4line[8]
            # each message sentence must have more than 5 words
            if len(proce_sen.split(' '))>=5:
                if conv_id not in future_train_conv_messages:
                    future_train_conv_messages[conv_id] = {messa_id:{'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon04'}, 'messa_num':1}
                else: 
                    future_train_conv_messages[conv_id]['messa_num']+=1
                    future_train_conv_messages[conv_id][messa_id] = {'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon04'}
    
    future_test_conv_messages = {}
    for mon5line in testmon5lines:
        mon5line = mon5line.strip('\n').split('\t')
        if len(mon5line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon5line[0], mon5line[1], mon5line[2], mon5line[3], mon5line[4], mon5line[5], mon5line[6], mon5line[7], mon5line[8]
            # each message sentence must have more than 5 words
            if len(proce_sen.split(' '))>=5:
                if conv_id not in future_test_conv_messages:
                    future_test_conv_messages[conv_id] = {messa_id:{'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon05'}, 'messa_num':1}
                else: 
                    future_test_conv_messages[conv_id]['messa_num']+=1
                    future_test_conv_messages[conv_id][messa_id] = {'messa_text':str(proce_sen), 'uid':str(uid), 'time':'mon05'}

    ##################################################################################past_train+future_train_future_test:user_message_dict####################################
    past_train_user_messages = {}
    for mon1line in mon1lines:
        mon1line = mon1line.strip('\n').split('\t')
        if len(mon1line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon1line[0], mon1line[1], mon1line[2], mon1line[3], mon1line[4], mon1line[5], mon1line[6], mon1line[7], mon1line[8]
            # each message sentence must have more than 5 words
            if len(proce_sen.split(' '))>=5:
                if uid not in past_train_user_messages:
                    past_train_user_messages[uid] = {messa_id:{'messa_text':str(proce_sen), 'conv_id':str(conv_id), 'time':'mon01'}, 'messa_num':1}
                else: 
                    past_train_user_messages[uid]['messa_num']+=1
                    past_train_user_messages[uid][messa_id] = {'messa_text':str(proce_sen), 'conv_id':str(conv_id), 'time':'mon01'}
    for mon2line in mon2lines:
        mon2line = mon2line.strip('\n').split('\t')
        if len(mon2line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon2line[0], mon2line[1], mon2line[2], mon2line[3], mon2line[4], mon2line[5], mon2line[6], mon2line[7], mon2line[8]
            # each message sentence must have more than 5 words
            if len(proce_sen.split(' '))>=5:
                if uid not in past_train_user_messages:
                    past_train_user_messages[uid] = {messa_id:{'messa_text':str(proce_sen), 'conv_id':str(conv_id), 'time':'mon02'}, 'messa_num':1}
                else: 
                    past_train_user_messages[uid]['messa_num']+=1
                    past_train_user_messages[uid][messa_id] = {'messa_text':str(proce_sen), 'conv_id':str(conv_id), 'time':'mon02'}
    for mon3line in mon3lines:
        mon3line = mon3line.strip('\n').split('\t')
        if len(mon3line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon3line[0], mon3line[1], mon3line[2], mon3line[3], mon3line[4], mon3line[5], mon3line[6], mon3line[7], mon3line[8]
            # each message sentence must have more than 5 words
            if len(proce_sen.split(' '))>=5:
                if uid not in past_train_user_messages:
                    past_train_user_messages[uid] = {messa_id:{'messa_text':str(proce_sen), 'conv_id':str(conv_id), 'time':'mon03'}, 'messa_num':1}
                else: 
                    past_train_user_messages[uid]['messa_num']+=1
                    past_train_user_messages[uid][messa_id] = {'messa_text':str(proce_sen), 'conv_id':str(conv_id), 'time':'mon03'}
    future_train_user_messages = {}
    for mon4line in mon4lines:
        mon4line = mon4line.strip('\n').split('\t')
        if len(mon4line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon4line[0], mon4line[1], mon4line[2], mon4line[3], mon4line[4], mon4line[5], mon4line[6], mon4line[7], mon4line[8]
            # each message sentence must have more than 5 words
            if len(proce_sen.split(' '))>=5:
                if uid not in future_train_user_messages:
                    future_train_user_messages[uid] = {messa_id:{'messa_text':str(proce_sen), 'conv_id':str(conv_id), 'time':'mon04'}, 'messa_num':1}
                else: 
                    future_train_user_messages[uid]['messa_num']+=1
                    future_train_user_messages[uid][messa_id] = {'messa_text':str(proce_sen), 'conv_id':str(conv_id), 'time':'mon04'}
    future_test_user_messages = {}
    for mon5line in testmon5lines:
        mon5line = mon5line.strip('\n').split('\t')
        if len(mon5line) == 9:
            conv_id, messa_id, parent_id, ori_sen, proce_sen, uid, ptime, upnum, downnum = mon5line[0], mon5line[1], mon5line[2], mon5line[3], mon5line[4], mon5line[5], mon5line[6], mon5line[7], mon5line[8]
            # each message sentence must have more than 5 words
            if len(proce_sen.split(' '))>=5:
                if uid not in future_test_user_messages:
                    future_test_user_messages[uid] = {messa_id:{'messa_text':str(proce_sen), 'conv_id':str(conv_id), 'time':'mon05'}, 'messa_num':1}
                else: 
                    future_test_user_messages[uid]['messa_num']+=1
                    future_test_user_messages[uid][messa_id] = {'messa_text':str(proce_sen), 'conv_id':str(conv_id), 'time':'mon05'}
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
    print('\n')
    print(len(new_past_train_user_list))
    print(len(new_future_train_user_list))
    print(len(new_future_test_user_list))
    print(len(unify_user_conv_interact_list.keys()))




    

if __name__ =='__main__':
    
    data_dir='./online_conv_data/'
    
    read_all(data_dir=data_dir, topic_category='funny')

