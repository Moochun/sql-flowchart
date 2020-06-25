#!/usr/bin/env python
# coding: utf-8
# %%
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where
from sqlparse.tokens import Keyword, DML, DDL, Text
import re
import copy
from sqlparsemodel.Querystate import Querystate
Querystate = Querystate()

# %%
# from Querystate import Querystate
# Querystate = Querystate()
# Querystate.get_specialstate

# %% [markdown]
# ## Class Sql structure
# 建立 SQL組織圖的 類別 by package sqlparse

# %%



class Sqlstructure():
    
    def __init__(self, the_query):
        # 01判斷子查詢遞迴
        self.subquerys_dict = self.__split_subquery( the_query ,
                                              state = "MAIN",
                                              subquery_dict_params = {})
        
        # 02子查詢取代KEY NAME Function (取代為新的subquery)
        self.replaced_dict = self.__subquey_replace(self.subquerys_dict)
        
        # 03抓出所有功能的名稱及TABLE NAME (做成1個字典檔)
        self.structured_dict = {}
        for item in self.replaced_dict.items():
            self.structured_dict[ item[0] ] = self.__get_token_dict(  item[1]  )




# %% [markdown]
# # SPLIT : 判斷子查詢遞迴Function
# ## Function 子查詢分割
# ### 判斷子查詢相關SQL功能
# - SELECT : 判斷ttype is DML + 值為 "SELECT"
# - FROM : 判斷ttype is Keyword + 值為 "FROM"
# - WHERE : 判斷instance is Where + 值含 "WHERE"
#
# ### 紀錄上述state作為後續標註
# - state ：初始SQL狀態。樣式為 # MAIN、SELECT、FROM、WHERE再持續往上加
# - statement : 當前SQL狀態。樣式為 # MAIN、SELECT、FROM、WHERE再持續往上加
# - count_dict：紀錄不同樣式的子查詢出現次數。(作為ID編碼 4.會用到)
#
# ### 觀察是否有子查詢
# - is_subselect : 檢查token是否有子查詢, 
#  - 回傳 boolean 
# - trace_token_is_subselect ： 有些功能語句會在Parenthesis外包裝一個Identifier，需要再往下追蹤一層。
#  - 回傳dictionary，KEY : 標註當前SQL狀態的次數、VALUE:Token訊息。
#  
# ### 將回傳的Token Value依照標註儲存於DICT
# - 儲存KEY - new_state：state + "_" + state + (count_dict[statement])
# - 儲存Value - token.value
# - 儲存的字典檔：subquery_dict
#
# ### 遞迴功能(將子查詢持續往下查找分成 1 class and 2nd class)
# - 遞迴參數：
#  - the_query = 儲存Value並去掉頭尾的「括號」(4.), 
#  - state = new_state(4.), 
#  - subquery_dict_params = 儲存的字典檔(4.)
# - 遞迴結束條件：
#  - sqiparse樹結構 1 層Trace完後，無任何有SELECT語句的子查詢。

# %%


    def __split_subquery(self, the_query, state = "MAIN", subquery_dict_params = None):
    
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
        count_dict = copy.deepcopy(Querystate.subquery_statelist) # list need to be copied to avoid using same List
        statement = ""
        for token in stmt.tokens:

            # 02 Setting state
            statement = Querystate.get_tokenstate(token, statement) # MAIN、SELECT、FROM、WHERE (if no revised keep the values)

            # 03 is_subselect check 
            ## Record statement count and Print subselect position 
            ### 03_1 1st class trace
            ### Save subquerys and recursive
            if (self.__is_subselect(token)):
                count_dict[statement] += 1
                the_subquery = token.value # Delete Captain and tail (parentheses)
                the_subtoken = token.tokens
                new_state = state + "_" + statement + str(count_dict[statement]) # Subselect key set new state
                subquery_dict[new_state] =  the_subquery
                self.__split_subquery(the_query = the_subquery[1:-1], 
                               state = new_state, 
                               subquery_dict_params = subquery_dict)
                continue ## 1st class show do not need go to 2nd class

            ### 03_2 2nd class trace    
            ### Save subquerys and recursive 
            subselect_dict = {}
            subselect_dict = self.__trace_token_is_subselect(token, count_dict, statement) 
            for sub_item in subselect_dict.items():
                the_subquery = sub_item[1].value # Delete Captain and tail (parentheses)
                the_subtokens = sub_item[1]
                new_state = state + "_" + statement + str(sub_item[0]) # Subselect key set new state
                subquery_dict[new_state] = the_subquery
                print(new_state)
                print(the_subquery)
                ### recirsive subselect and Update the subquery_dict
                self.__split_subquery(the_query = the_subquery[1:-1], 
                               state = new_state, 
                               subquery_dict_params = subquery_dict)


        return(subquery_dict)



# %% [markdown]
# ## Function : 是否有子查詢

# %%
    def __is_subselect(self, parsed):
        if not parsed.is_group:
            return False
        for item in parsed.tokens:
            if item.ttype is DML and item.value.upper() == 'SELECT':
                return True
        return False

# %% [markdown]
# ## Function : 追蹤token內的列表並確認是否有子查

# %%
    def __trace_token_is_subselect(self, parsed, count_dict, state):
        subselect_dict = {} # count_dict as KEY, item as VALUE

        ## No group return null subquery_dict
        if not parsed.is_group:
            return subselect_dict

        ## With group return Record subquery_dict
        for item in parsed.tokens:
            if self.__is_subselect(item):
                count_dict[state] += 1
                subselect_dict[ count_dict[state] ] = item

        return subselect_dict

# %% [markdown]
# # REPLACE: 子查詢取代KEY NAME Function
# ## Function : 取代子查詢為新的SUBQ.DBNAME
# ### 紀錄各項查詢LEVEL
# - subq_counts : "_" 次數決定子查詢所在等級(0:主查詢、1:1st子查詢、2:2nd子查詢)
# - subq_values : 取得子查詢列表的 sql表示
# - subq_keys : 取得子查詢列表的 KEY標註 表示 ("MAIN", "MAIN_SELECT1", ....)
#
# ### 兩迴圈分別檢視每一個 subq_values與下一層子查詢是否需要置換
# - this_level : subq_counts[idx] ， 子查詢所在等級
# - next_level : this_level + 1 ，子查詢下一層等級
# - next_level_check : 觀察兩件事
#  - 是否為下一層：subq_counts[idx_2nd]判斷是否等於 next level
#  - 上下層等級是否相關 : ex: MAIN 與 MAIN_SELECT1相關、
#    但MAIN_WHERE與MAIN_SELECT_1不相關
#    
# ### 置換KEY NAME 的VALUE by 第二層的KEYNAME，並加上SUBQ.作為子查詢標註
# 第二層KEYNAME : MAIN_FROM1、MAIN_WHERE1、MAIN_FROM1_FROM1....
# - 範例：(subquery) → (MAIN_FROM1) → (SUBQ.MAIN_FROM1)
# - 置換括號：(SUBQ.MAIN_FROM1) → SUBQ.MAIN_FROM1
#
#

# %%
    def __subquey_replace (self, subquerys_list = None):

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
                        " SUBQ."+ next_key,
                        subquerys[this_key])

                    print(subquerys[this_key])
        return(subquerys)

# %% [markdown]
#
#
# # GETINFO : 抓出所有功能的名稱及TABLE NAME 
# ## Function : get_token_dict ()  取得Token資訊的字典檔。
# ### 設定基本變數
#  - stmt : parsed query 已解析過的查詢語句
#  - statement : token的state 在 query_signs 又出現的。(get_tokenstate可以判別出來的)
#    "TABLE", "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", OTHER
#  - token_dict : 記錄所有token的Value及細部token。
#  - query_signs : get_tokenstate內判別的項目
#  
# ### 所有Token逐一判斷
#  - 00 Skip White space and Newline : 判斷 .ttype 決定是否為空白及換行
#  - 01 Setting state : 利用get_tokenstate記錄目前的語句狀態。
#  - 02 Investigate state and insert Values : 將各個 query sign 的文字結果(.value)儲存於字典檔(token_dict)裡
#  - 03 FROM or TABLE state get table names : "FROM" 語句下抓出所有的table名稱
#   - 1. 判斷：判斷isinstance(token, Identifier) 是否為 Identifier。(Identifier才會存table name)
#   - 2. 抓取：.get_alias()、.get_parent_name()、.get_real_name() 抓取 (匿名、資料庫名、資料表名)
#   - 3. 儲存：token_dict -> "FROM" -> token 中。

# %%
    def __get_token_dict(self, this_query):

        res = sqlparse.parse(this_query)
        stmt = res[0]

        statement = ""
        statement_special = ""
        token_dict = {} # need to return 
        query_signs = Querystate.gettoken_statelist # by get_tokenstate SHOW signs.

        for token in stmt.tokens:

            # 00 Skip White space and Newline
            if token.ttype is Text.Whitespace or token.ttype is Text.Whitespace.Newline:
                continue

            # 01 Setting state
            statement = Querystate.get_tokenstate(token, statement) # MAIN、SELECT、FROM、WHERE (if no revised keep the values)
            statement_special = Querystate.get_specialstate(token, statement_special)# ON、JOIN (Special State)
            
            # 02 Investigate state and insert Values (strip the "token.value" )
            ## 02_1 Only Query sign(tokenstate) token -> <Skip>
            ## 02_2 1st Insert token 
            ## 02_3 2nd... Inser token 
            if token.value.upper() in query_signs : 
                continue
            elif statement not in list(token_dict.keys()) :
                token_dict[statement] = {"value" : re.sub("\n\s+", "", token.value.strip()) } # Replace \n and after space
            else :
                token_dict[statement]["value"] += (" " + re.sub("\n\s+", "", token.value.strip()))# Replace \n and after space

            # 03 FROM or TABLE state get table names
            # 03_0 Setting Special state and check 
            ## 03_1 Find Idntifier (Idntifier need use isinstance to check)
            ## 03_2 get alias_name, db_name, real_name
            ## 03_3 save alias_name, db_name, real_name
            if (statement == "FROM" or statement == "TABLE") and isinstance(token, Identifier):# identifier check and do something 
                
                # 03_0 Setting Special state and check 
                if statement_special == "ON" :
                    continue
                    
                # 03_2 get names
                alias_name = token.get_alias()
                db_name = token.get_parent_name()
                real_name = token.get_real_name()

                if "token" not in list(token_dict[statement].keys()) :
                    token_dict[statement]["token"] = [[alias_name, db_name, real_name]]
                else :
                    token_dict[statement]["token"].append([alias_name, db_name, real_name])
                    
            if (statement == "FROM" or statement == "TABLE") and isinstance(token, IdentifierList): # [from AAA, BBB] will be identifierlist check and do something 
                for subtoken in token.tokens : 
                    # 03_0 Setting Special state and check 
                    if  isinstance(subtoken, Identifier) : # identifier check and do something 
                        if statement_special == "ON" :
                            continue

                        # 03_2 get names
                        alias_name = subtoken.get_alias()
                        db_name = subtoken.get_parent_name()
                        real_name = subtoken.get_real_name()

                        if "token" not in list(token_dict[statement].keys()) :
                            token_dict[statement]["token"] = [[alias_name, db_name, real_name]]
                        else :
                            token_dict[statement]["token"].append([alias_name, db_name, real_name])
                    
        return token_dict

# %% [markdown]
# # A0傳值與傳址的差異
# python 與其他OOP的程式語言不同，他都是以參照為基礎，而不是以值給予作為轉換。
# ex: a = b ： 把a的位址連結到b上，而不是把a的值複製一份到b上。
# 且都是以物件的方式儲存!
# - deepcopy : 將所有物件型態全數複製到新的位址上。
# https://codertw.com/%E7%A8%8B%E5%BC%8F%E8%AA%9E%E8%A8%80/531439/
#
# # A1TTFlow chart 層級======
#
# ---------
# 1. TABLE NAMES (1:N)
# 2. FROM - 完整 join 表 (1)
# 3. WHER - 完整篩選條件列表(1)
# 4. GROUP BY - 完整group by 列表 (1)
# 5. ORDER BY - 完整order by 列表 (1)
# 6. limit - 數字 (1)
# 7. SELECT - 選取欄位列表 (1) 
# 8. CREATE TABLE NAME
