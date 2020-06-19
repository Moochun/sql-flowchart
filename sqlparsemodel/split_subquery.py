#!/usr/bin/env python
# coding: utf-8
# %%

# # 01判斷子查詢遞迴Function
# ### 1.判斷子查詢相關SQL功能
# - SELECT : 判斷ttype is DML + 值為 "SELECT"
# - FROM : 判斷ttype is Keyword + 值為 "FROM"
# - WHERE : 判斷instance is Where + 值含 "WHERE"
# 
# ### 2. 紀錄上述state作為後續標註
# - state ：初始SQL狀態。樣式為 # MAIN、SELECT、FROM、WHERE再持續往上加
# - statement : 當前SQL狀態。樣式為 # MAIN、SELECT、FROM、WHERE再持續往上加
# - count_dict：紀錄不同樣式的子查詢出現次數。(作為ID編碼 4.會用到)
# 
# ### 3. 觀察是否有子查詢
# - is_subselect : 檢查token是否有子查詢, 
#  - 回傳 boolean 
# - trace_token_is_subselect ： 有些功能語句會在Parenthesis外包裝一個Identifier，需要再往下追蹤一層。
#  - 回傳dictionary，KEY : 標註當前SQL狀態的次數、VALUE:Token訊息。
#  
# ### 4. 將回傳的Token Value依照標註儲存於DICT
# - 儲存KEY - new_state：state + "_" + state + (count_dict[statement])
# - 儲存Value - token.value
# - 儲存的字典檔：subquery_dict
# 
# ### 5. 遞迴功能(將子查詢持續往下查找分成 1 class and 2nd class)
# - 遞迴參數：
#  - the_query = 儲存Value並去掉頭尾的「括號」(4.), 
#  - state = new_state(4.), 
#  - subquery_dict_params = 儲存的字典檔(4.)
# - 遞迴結束條件：
#  - sqiparse樹結構 1 層Trace完後，無任何有SELECT語句的子查詢。

# %%


import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where
from sqlparse.tokens import Keyword, DML, DDL
import re
import copy


# %%


def split_subquery(the_query, state = "MAIN", subquery_dict_params = None):
    
    # Redefine object 
    if not subquery_dict_params:
        subquery_dict_params = {}
    
    # Function local variables and codes 
    # Record query's tokens
    subquery_dict = subquery_dict_params
    print(subquery_dict)
    res = sqlparse.parse(the_query)
    stmt = res[0]
    subquery_dict[state] = the_query
    print(stmt._pprint_tree())

    # 01 Record subquery in differnent statement out times
    count_dict = {"MAIN" : 0, "SELECT" : 0, "FROM" : 0, "WHERE" : 0}
    statement = ""
    for token in stmt.tokens:

        # 02 Setting state
        statement = get_tokenstate(token, statement) # MAIN、SELECT、FROM、WHERE (if no revised keep the values)

        # 03 is_subselect check 
        ## Record statement count and Print subselect position 
        ### 03_1 1st class trace
        ### Save subquerys and recursive
        if (is_subselect(token)):
            count_dict[statement] += 1
            the_subquery = token.value # Delete Captain and tail (parentheses)
            the_subtoken = token.tokens
            new_state = state + "_" + statement + str(count_dict[statement]) # Subselect key set new state
            subquery_dict[new_state] =  the_subquery
            split_subquery(the_query = the_subquery[1:-1], 
                           state = new_state, 
                           subquery_dict_params = subquery_dict)
            continue ## 1st class show do not need go to 2nd class

        ### 03_2 2nd class trace    
        ### Save subquerys and recursive 
        subselect_dict = {}
        subselect_dict = trace_token_is_subselect(token, count_dict, statement) 
        for sub_item in subselect_dict.items():
            the_subquery = sub_item[1].value # Delete Captain and tail (parentheses)
            the_subtokens = sub_item[1]
            new_state = state + "_" + statement + str(sub_item[0]) # Subselect key set new state
            subquery_dict[new_state] = the_subquery
            print(new_state)
            print(the_subquery)
            ### recirsive subselect and Update the subquery_dict
            split_subquery(the_query = the_subquery[1:-1], 
                           state = new_state, 
                           subquery_dict_params = subquery_dict)
  
        
    return(subquery_dict)


# ## Function : Token的狀態回傳
# MAIN、SELECT、FROM、WHER

# %%


def get_tokenstate(token, statement):
    ## 01 TABLE ========================
    if token.ttype is Keyword and token.value.upper() == "TABLE":
        statement = "TABLE"
        print("SELECT表示句:" + token.value.upper())
    
    ## 02 SELECT ========================
    elif token.ttype is DML and token.value.upper() == "SELECT":
        statement = "SELECT"
        print("SELECT表示句:" + token.value.upper())
    
    ## 03 FROM ========================    
    elif token.ttype is Keyword and token.value.upper() == "FROM":
        statement = "FROM"
        print("FROM表示句:" + token.value.upper())
    
    ## 04 WHERE ========================    
    elif isinstance(token, Where) and bool(re.search("WHERE",token.value.upper())):
        statement = "WHERE"
        print("WHERE表示句:" + token.value.upper())
    
    ## 05 GROUP BY ========================    
    elif token.ttype is Keyword and token.value.upper() == "GROUP BY":
        statement = "GROUPBY"
        print("GROUPBY表示句:" + token.value.upper())
        
    ## 06 ORDER BY ========================    
    elif token.ttype is Keyword and token.value.upper() == "ORDER BY":
        statement = "ORDERBY"
        print("ORDERBY表示句:" + token.value.upper())
    
    ## 07 LIMIT ========================    
    elif token.ttype is Keyword and token.value.upper() == "LIMIT":
        statement = "LIMIT"
        print("LIMIT表示句:" + token.value.upper())
        
    ## 00 OTHERS (FROM SELECT的抓取範圍過大會判別到 leftjoin、innerjoin、on等keyword)========================    
    elif token.ttype is Keyword and statement != "FROM" and statement != "SELECT":
        statement = "OTHER"
        print("OTHER表示句:" + token.value.upper())
    return statement


# ## Function : 是否有子查詢

# %%


def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


# ## Function : 追蹤token內的列表並確認是否有子查

# %%


def trace_token_is_subselect(parsed, count_dict, state):
    subselect_dict = {} # count_dict as KEY, item as VALUE
    
    ## No group return null subquery_dict
    if not parsed.is_group:
        return subselect_dict
    
    ## With group return Record subquery_dict
    for item in parsed.tokens:
        if is_subselect(item):
            count_dict[state] += 1
            subselect_dict[ count_dict[state] ] = item
    
    return subselect_dict


# %%


the_query = '''CREATE TABLE FINAL as 
               select a1, count(a2) 
               from MEOW.AAA_table as t3 
                    left join (SELECT * FROM (SELECT * FROM MEOW.CCC_table ) as t1 ) as t2 on (t3.a1 = t2.a5)
               where t3.X1 = 'thing' and t3.X2 in (SELECT distinct(b1) FROM MEOW.BBB_table ) 
               Group by a1---
               limit 15'''
sql = """
    select K.a,K.b from (select H.b from (select G.c from (select F.d from
    (select E.e from MEOW.A as t1 , MEOW.B, MEOW.C, MEOW.D, MEOW.E), F), G), H), I, J, K order by 1,2;
    """

subquerys_list = split_subquery( the_query ,
                  state = "MAIN",
                  subquery_dict_params = {})


# # 02子查詢取代KEY NAME Function
# ### 1.紀錄各項查詢LEVEL
# - subq_counts : "_" 次數決定子查詢所在等級(0:主查詢、1:1st子查詢、2:2nd子查詢)
# - subq_values : 取得子查詢列表的 sql表示
# - subq_keys : 取得子查詢列表的 KEY標註 表示 ("MAIN", "MAIN_SELECT1", ....)
# 
# ### 2. 兩迴圈分別檢視每一個 subq_values與下一層子查詢是否需要置換
# - this_level : subq_counts[idx] ， 子查詢所在等級
# - next_level : this_level + 1 ，子查詢下一層等級
# - next_level_check : 觀察兩件事
#  - 是否為下一層：subq_counts[idx_2nd]判斷是否等於 next level
#  - 上下層等級是否相關 : ex: MAIN 與 MAIN_SELECT1相關、
#    但MAIN_WHERE與MAIN_SELECT_1不相關
#    
# ### 3. 置換KEY NAME 的VALUE by 第二層的KEYNAME，並加上SUBQ.作為子查詢標註
# 第二層KEYNAME : MAIN_FROM1、MAIN_WHERE1、MAIN_FROM1_FROM1....
# - 範例：(subquery) → (MAIN_FROM1) → (SUBQ.MAIN_FROM1)
# - 置換括號：(SUBQ.MAIN_FROM1) → SUBQ.MAIN_FROM1
# 
# # 傳值與傳址的差異
# python 與其他OOP的程式語言不同，他都是以參照為基礎，而不是以值給予作為轉換。
# ex: a = b ： 把a的位址連結到b上，而不是把a的值複製一份到b上。
# 且都是以物件的方式儲存!
# - deepcopy : 將所有物件型態全數複製到新的位址上。
# https://codertw.com/%E7%A8%8B%E5%BC%8F%E8%AA%9E%E8%A8%80/531439/

# %%


import re
import copy


# %%


def subquey_replace (subquerys_list = None):
    
    # Redefine object 
    if not subquerys_list:
        subquerys_list = {}
    subquerys = copy.deepcopy(subquerys_list)

    # https://stackoverflow.com/questions/3895646/number-of-regex-matches
    # Get "_" count to confirm the class level (1st, 2nd ,3rd ...)
    subq_counts = [len(re.findall("_", subq)) for subq in subquerys.keys()]
    subq_values = list(subquerys.values())
    subq_keys = list(subquerys.keys())

    for idx in range(len(subq_keys)):
        this_query = subq_values[idx]
        this_key = subq_keys[idx]
        this_level = subq_counts[idx]
        next_level = this_level + 1
        print(next_level)
        print("================")

        for idx_2nd in range(len(subq_counts)):
            next_query = subq_values[idx_2nd]
            next_key = subq_keys[idx_2nd]
            next_level_check =  len ( re.findall ( this_key, subq_keys[idx_2nd] ) ) 
            if subq_counts[idx_2nd] == next_level and next_level_check > 0:
                print(next_key+"===========")
                print(subquerys[this_key])
                print(next_query)
                ## replace the KEY name to origin Subquery
                ## Replace next level (2nd) statement to first level
                ### first level : this_query 
                ### second_level : next_query
                ### replace_name : next_key
                subquerys[this_key] =  subquerys[this_key].replace(
                                             next_query, 
                                             next_key)
                ## subq_key NAME Remove Parenthesis and Add SUBQ. databasename 
                subquerys[this_key] = re.sub(
                    "\\(" + next_key + "\\)",
                    "SUBQ."+ next_key,
                    subquerys[this_key])

                print(subquerys[this_key])
    return(subquerys)


# %%


def __main__():
    the_query = '''CREATE TABLE FINAL as 
               select a1, count(a2) 
               from MEOW.AAA_table as t3 
                    left join (SELECT * FROM (SELECT * FROM MEOW.CCC_table ) as t1 ) as t2 on (t3.a1 = t2.a5)
               where t3.X1 = 'thing' and t3.X2 in (SELECT distinct(b1) FROM MEOW.BBB_table ) 
               Group by a1
               limit 15'''
    subquerys_list = split_subquery( the_query ,
                  state = "MAIN",
                  subquery_dict_params = {})
    ## MAIN
    subquey_replace(subquerys_list)


# %%


the_query = '''CREATE TABLE FINAL as 
               select a1, count(a2) 
               from MEOW.AAA_table as t3 
                    left join (SELECT * FROM (SELECT * FROM MEOW.CCC_table ) as t1 ) as t2 on (t3.a1 = t2.a5)
               where t3.X1 = 'thing' and t3.X2 in (SELECT distinct(b1) FROM MEOW.BBB_table ) 
               Group by a1---
               limit 15'''
sql = """
    select K.a,K.b from (select H.b from (select G.c from (select F.d from
    (select E.e from MEOW.A as t1 , MEOW.B, MEOW.C, MEOW.D, MEOW.E), F), G), H), I, J, K order by 1,2;
    """

subquerys_list = split_subquery( the_query ,
                  state = "MAIN",
                  subquery_dict_params = {})
## MAIN
subquey_replace(subquerys_list)

