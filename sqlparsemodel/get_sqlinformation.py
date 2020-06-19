#!/usr/bin/env python
# coding: utf-8

# In[87]:


from sqlparsemodel.split_subquery import split_subquery
from sqlparsemodel.split_subquery import subquey_replace
from sqlparsemodel.split_subquery import get_tokenstate
from sqlparse.tokens import Text
from sqlparse.sql import Identifier
import sqlparse
import re


# # Flow chart 層級======
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
# 
# # 抓出所有功能的名稱及TABLE NAME 
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

# In[105]:


def get_token_dict(this_query):
    
    res = sqlparse.parse(this_query)
    stmt = res[0]
    
    statement = ""
    token_dict = {} # need to return 
    query_signs = ["TABLE", "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT"] # by get_tokenstate SHOW signs.
    
    for token in stmt.tokens:

        # 00 Skip White space and Newline
        if token.ttype is Text.Whitespace or token.ttype is Text.Whitespace.Newline:
            continue

        # 01 Setting state
        statement = get_tokenstate(token, statement) # MAIN、SELECT、FROM、WHERE (if no revised keep the values)

        # 02 Investigate state and insert Values (strip the "token.value" )
        ## 02_1 Only Query sign(tokenstate) token -> <Skip>
        ## 02_2 1st Insert token 
        ## 02_3 2nd... Inser token 
        if token.value.upper() in query_signs : 
            continue
        elif statement not in list(token_dict.keys()) :
            token_dict[statement] = {"value" : token.value.strip()}
        else :
            token_dict[statement]["value"] += (" " + token.value.strip())

        # 03 FROM or TABLE state get table names
        ## 02_1 Find Idntifier (Idntifier need use isinstance to check)
        ## 02_2 get alias_name, db_name, real_name
        ## 02_3 save alias_name, db_name, real_name
        if (statement == "FROM" or statement == "TABLE") and isinstance(token, Identifier):
            # 02_2 get names
            alias_name = token.get_alias()
            db_name = token.get_parent_name()
            real_name = token.get_real_name()

            if "token" not in list(token_dict[statement].keys()) :
                token_dict[statement]["token"] = [[alias_name, db_name, real_name]]
            else :
                token_dict[statement]["token"].append([alias_name, db_name, real_name])
    return token_dict


# In[106]:


def __main__():
    the_query = '''CREATE TABLE WORK.FINAL as 
                   select a1, count(a2), a3 
                   from AAA_table
                        left join (SELECT * FROM (SELECT * FROM MEOW.CCC_table ) as t1 ) as t2 on (t3.a1 = t2.a5)
                   where t3.X1 = 'thing' and t3.X2 in (SELECT distinct(b1) FROM MEOW.BBB_table ) 
                   Group by t3.a1, t3.a3
                   order by t3.a1, t3.a3
                   limit 15'''
    subquerys_list = split_subquery( the_query ,
                      state = "MAIN",
                      subquery_dict_params = {})
    ## MAIN
    subquery_all = subquey_replace(subquerys_list)
    subquery_struct = {}
    for item in subquery_all.items():
        subquery_struct[ item[0] ] = get_token_dict(  item[1]  )
    subquery_struct


# In[ ]:




