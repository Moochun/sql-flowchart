#!/usr/bin/env python
# coding: utf-8
# %%
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where
from sqlparse.tokens import Keyword, DML, DDL
import re
# ## CLASS : Token的狀態回傳及記錄
#TABLE、SELECT、FROM、WHERE、GROUPBY、ORDERBY、LIMIT、OTHER

# %%
class Querystate():
    
    def __init__(self):
        self.subquery_statelist = {"MAIN" : 0, "SELECT" : 0, "FROM" : 0, "WHERE" : 0}
        self.gettoken_statelist = ["TABLE", "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "HAVING"]
        self.nodeorder_statelist = ["FROM", "WHERE", "GROUP BY", "HAVING", "SELECT", "ORDER BY", "LIMIT", "TABLE" ]



# %% [markdown]
# ## Function : Token的狀態回傳
# #TABLE、SELECT、FROM、WHERE、GROUPBY、ORDERBY、LIMIT、HAVING、OTHER

# %%
    def get_tokenstate(self, token, statement):
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
            
        ## 08 HAVING ========================    
        elif token.ttype is Keyword and token.value.upper() == "HAVING":
            statement = "HAVING"
            print("HAVING表示句:" + token.value.upper())

        ## 00 OTHERS (FROM的抓取範圍過大會判別到 leftjoin、innerjoin、on等keyword)========================
        # (FROM 的抓取範圍過大會判別到 leftjoin、innerjoin、on等keyword)
        # (SELECT 的抓取範圍過大會判別到 DISTINCT 等keyword)
        # (HAVING 的抓取範圍過大會判別到 AND OR  等keyword)
        elif token.ttype is Keyword and statement != "FROM" and statement != "SELECT" and statement != "HAVING":
            statement = "OTHER"
            print("OTHER表示句:" + token.value.upper())
        return statement

# %% [markdown]
# ## Function : Special State 回傳

# %%
    def get_specialstate(self, token, statement):
        ## 01 ON ========================
        if token.ttype is Keyword and token.value.upper() == "ON":
            statement = "ON"
            print("ON表示句:" + token.value.upper())
            
        ## 02 JOIN ========================
        if token.ttype is Keyword and bool(re.search("JOIN",token.value.upper())):
            statement = "JOIN"
            print("JOIN表示句:" + token.value.upper())
        return statement
