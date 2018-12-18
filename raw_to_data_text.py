import pandas as pd
import re
import json

# 読み込むファイルを変えるときはここを変える
data_path = 'raw_data/channels_0.csv'

# でてくるファイル名を変えるときはここを変える
output_json = 'channel_0_text.json'

# 1.raw_dataの読み込み
raw = pd.read_csv(data_path, encoding='utf_8')

# 2.nodeのリストを作るためにfrom_idとto_idのリストを取得
nodes_1 = pd.DataFrame(raw['from_id'].unique(), columns = ['name'])
nodes_2_bef = pd.DataFrame(raw['to_id'].unique(), columns = ['name'])

# 3.to_idは送信先一覧のリストなので、そのままでは使えない
# ⇒1行ずつループに流して分解する
nodes_2 = pd.DataFrame([], columns = ['name'])

for index, Series in nodes_2_bef.iterrows():
    to_list = re.findall('[a-z0-9]+', Series['name'])
    for tmp in to_list:
        temp = pd.Series([tmp], index = ['name'])
        nodes_2 = nodes_2.append(temp, ignore_index=True)

nodes_2 = pd.DataFrame(nodes_2['name'].unique(), columns = ['name'] )

# 4.from_idとto_idのかぶりを取り除きnodeリストを完成させる
nodes = pd.concat([nodes_1,nodes_2])
nodes.reset_index()
nodes = nodes.replace('nan', "a").fillna("a") #id=NaNのものがあったので、とりあえずaを入れておく
nodes = pd.DataFrame(nodes['name'].unique(), columns = ['name'])

# 5.linkリストを作るためrawデータから読みだす
links_bef = raw[['from_id', 'to_id']].fillna("a")

# 6.to_idのリストをばらす
links = pd.DataFrame([], columns = ['from_id', 'to_id'])

for index, Series in links_bef.iterrows():
    to_list = re.findall('[a-z0-9]+', Series['to_id'])
    for to in to_list:
        tmp = pd.Series([ Series['from_id'], to], index=links.columns )
        search = links[ (links['from_id'] == tmp['from_id']) & (links['to_id'] == tmp['to_id']) ]
        if len(search) == 0:
            links = links.append(tmp, ignore_index=True)

# 7.id=Nanをaで埋める処理
links_0 = links.replace('nan', "a").fillna("a")
nodes_0 = nodes.replace('nan', "a").fillna("a")

# 8.columnsの名前を変える
nodes_0 = nodes_0.rename(columns={'name':'pre_id'})

# 9.あとでidのおきかえをやるため、今のidをpreとする
links_0 = links_0.rename(columns={'from_id':'pre_source', 'to_id':'pre_target'})

# 10.各ノードに番号を振っていく
nodes_0['id'] = range(len(nodes_0))

# 11.この番号を使ってsourceとtargetを書き換える
links_0['source'] = 0
links_0['target'] = 0
for i in range(len(links_0)):
    links_0.iat[i,2] = list(nodes_0.query('pre_id == "' + links_0.iloc[i][0] + '"').index)[0]
    links_0.iat[i,3] = list(nodes_0.query('pre_id == "' + links_0.iloc[i][1] + '"').index)[0]

# 12.pre を削除
node = nodes_0.drop(['pre_id'] ,axis=1)
link = links_0.drop(['pre_source','pre_target'] ,axis=1)

# 13.会話の流れを取り出してJSONへ
temp = raw['content_text'].str.replace("\n", "\\n")
temp = temp.str.replace("\r", "\\r")
comment = pd.DataFrame(temp) # str アクセサをつけることで各要素について文字列処理ができる
comment['order'] = range(len(comment))
print(comment)

# 14.会話の方向データを作る処理
com_arrow_1 = pd.DataFrame(raw[['from_id','to_id']])
com_arrow_1['order'] = range(len(com_arrow_1))

# to_idの行先をばらすのが先
com_arrow_2 = pd.DataFrame([], columns = ['from_id', 'to_id', 'order'])

for index, Series in com_arrow_1.iterrows():
    to_list = re.findall('[a-z0-9]+', Series['to_id'])
    if to_list == []:
        to_list.append(-1)
    for to in to_list:
        tmp = pd.Series([ Series['from_id'], to, int(index)], index = com_arrow_2.columns )
        com_arrow_2 = com_arrow_2.append(tmp, ignore_index=True)

com_arrow_2 = com_arrow_2.replace('nan', "a").fillna("a")

# ばらしたあと、idに変換
for i in range(len(com_arrow_2)):
    com_arrow_2.iat[i,0] = list(nodes_0.query('pre_id == "' + com_arrow_2.iloc[i][0] + '"').index)[0]
    if com_arrow_2.iloc[i][1] != -1:
        com_arrow_2.iat[i,1] = list(nodes_0.query('pre_id == "' + com_arrow_2.iloc[i][1] + '"').index)[0]

com_arrow_2 = com_arrow_2.astype(int)

# 15.JSONファイルにして出力する
output = "node = \'" + node.to_json(orient='records') + "\';\r\nlink = \'" + link.to_json(orient='records') + "\';\r\nchat = \'" + com_arrow_2.to_json(orient='records') + "\';\r\ncomment = \'" + comment.to_json(orient="records") + "\';"

with open(output_json, 'w', encoding='utf_8') as f:
    f.write(output)

print(output)

# 16.終わり
print('おわり')