import json


# with open('./subTwiData/input_past_training_data.json', 'r', encoding='utf-8') as file:
#     input_past_training_data = json.load(file)

f = open('./subTwiData/input_past_training_data.json', 'r', encoding='utf-8')
input_past_training_data = f.readlines()
f = open('./subTwiData/input_future_training_data.json', 'r', encoding='utf-8')
input_future_training_data = f.readlines()
f = open('./subTwiData/input_future_test_data.json', 'r', encoding='utf-8')
input_future_test_data = f.readlines()

past_train_list = []
for each_past_train in input_past_training_data:
    each_past_train_dict = json.loads(each_past_train)
    past_train_list.append(each_past_train_dict)

future_train_list = []
for each_future_train in input_future_training_data:
    each_future_train_dict = json.loads(each_future_train)
    future_train_list.append(each_future_train_dict)

future_test_list = []
for each_future_test in input_future_test_data:
    each_future_test_dict = json.loads(each_future_test)
    future_test_list.append(each_future_test_dict)



past_train_user_list = {}
past_train_positive_tags = {}
for each_past_train in past_train_list:
    if each_past_train['label'] == 1:
        if each_past_train['user_id'] not in past_train_user_list:
            past_train_user_list[each_past_train['user_id']] = each_past_train['user_past_history_num']
        if each_past_train['hashtag'] not in past_train_positive_tags:
            past_train_positive_tags[each_past_train['hashtag']] = each_past_train['pos_tag_past_tweets_num']


print('past train user num: '+str(len(past_train_user_list.keys())))
print('past train tag num: '+str(len(past_train_positive_tags.keys())))


future_train_user_list = {}
future_train_positive_tags = {}
for each_future_train in future_train_list:
    if each_future_train['label'] == 1:
        if each_future_train['user_id'] not in future_train_user_list:
            future_train_user_list[each_future_train['user_id']] = each_future_train['user_future_history_num']
        if each_future_train['hashtag'] not in future_train_positive_tags:
            future_train_positive_tags[each_future_train['hashtag']] = each_future_train['pos_tag_future_tweets_num']


print('future train user num: '+str(len(future_train_user_list.keys())))
print('future train tag num: '+str(len(future_train_positive_tags.keys())))


future_test_user_list = {}
future_test_positive_tags = {}
for each_future_test in future_test_list:
    if each_future_test['label'] == 1:
        if each_future_test['user_id'] not in future_test_user_list:
            future_test_user_list[each_future_test['user_id']] = each_future_test['user_future_history_num']
        if each_future_test['hashtag'] not in future_test_positive_tags:
            future_test_positive_tags[each_future_test['hashtag']] = each_future_test['pos_tag_future_tweets_num']


print('future test user num: '+str(len(future_test_user_list.keys())))
print('future test tag num: '+str(len(future_test_positive_tags.keys())))




'''
import matplotlib.pyplot as plt
from collections import Counter

# 定义一个函数来统计并绘制折线图
def plot_number_frequencies(data, start=1, threshold=15):
    # 提取字典中的值
    values = list(data.values())

    # 使用Counter来计算每个数字出现的次数
    counter = Counter(values)

    # 初始化一个列表来存储从start到threshold-1以及大于等于threshold的频率
    frequency = [0] * (threshold - start + 1)  # 现在与labels长度一致

    # 计算每个数字出现的频率
    for number, count in counter.items():
        if start <= number < threshold:
            index = number - start
            frequency[index] = count
        elif number >= threshold:
            frequency[-1] += count  # 将大于等于threshold的所有计数加到最后一个位置

    # 创建x轴标签
    labels = [str(i) for i in range(start, threshold)] + [f'>={threshold}']

    # 绘制折线图
    plt.figure(figsize=(10, 6))
    plt.plot(labels, frequency, marker='o', linestyle='-', color='b')
    plt.xlabel('Number')
    plt.ylabel('Frequency')
    plt.title(f'Future Test of Numbers in Data (Start: {start}, Threshold: {threshold})')
    plt.grid(True)
    
    # 为x轴标签旋转以防止重叠
    plt.xticks(rotation=45)
    
    # 展示图形
    plt.tight_layout()  # 自动调整子图参数,使之填充整个图像区域
    plt.show()

# # 调用函数并设置起始点和阈值
# plot_number_frequencies(past_train_user_list, start=1, threshold=20)
        
# # 调用函数并设置起始点和阈值
# plot_number_frequencies(past_train_positive_tags, start=1, threshold=25)


# # 调用函数并设置起始点和阈值
# plot_number_frequencies(future_train_user_list, start=1, threshold=20)
        
# # 调用函数并设置起始点和阈值
# plot_number_frequencies(future_train_positive_tags, start=1, threshold=20)

# 调用函数并设置起始点和阈值
plot_number_frequencies(future_test_user_list, start=1, threshold=20)
        
# 调用函数并设置起始点和阈值
plot_number_frequencies(future_test_positive_tags, start=1, threshold=20)

'''
