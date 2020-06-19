#!/usr/bin/env python
# coding: utf-8
# %% [markdown]
# # CLASS SAS Structure

# %%
from sqlparsemodel.Sqlstructure import Sqlstructure
from sqlparsemodel.Querystate import Querystate
import re 
import pandas as pd 
Querystate = Querystate()

# %% [markdown]
# ## init function 
#
#


# %%
class Sastructure():
    
    def __init__(self, query):
        self.sql_parsed_list = self.__sassql_parsed( query )
        self.node_dict, self.node_property, self.from_to_property = self.__sassql_nodelist( self.sql_parsed_list )
        self.from_to_query = self.__query_relation( self.node_property )
        
        

# %% [markdown]
# ## From to ID check and replace 
# - 先放from_id : from_id == "" and to_id == "" 把now_id放到from_id
# - 再放to_id : from_id != "" and to_id == "" 把now_id放到to_id
# - id置換 : from_id != "" and to_id != "" to_id 取代 from_id，並把now_id放到_to_id

# %%
    def __from_to_check(self, from_id, to_id, now_id):
        if from_id == "" and to_id == "":
            from_id = now_id

        elif from_id != "" and to_id == "":
            to_id = now_id

        elif from_id != "" and to_id != "":
            from_id = to_id
            to_id = now_id

        return(from_id, to_id)

# %%
    def __none_to_empty(self, string):
        if string is None :
            return("")
        else:
            return(string)

# %%
    def __sassql_parsed(self, query):
        sql_parsed_list = {}
        query_id = 0
        for string in re.split("PROC SQL;", query):
            if string.strip() == "":
                continue
            query_id += 1
            print("=======================")
            print(re.sub("QUIT;", "", string).strip())
            sql_parsed_list[ str(query_id) ] = Sqlstructure(string)

        return( sql_parsed_list )

# %% [markdown]
# ## PROC SQLs to relation structure
# ### Setting dataframes and dictinary
# - NODE ID dataframe
# - NODE_ID dict
# - From to dataframe(NODE) 
# - From Create dataframe (tables NODE)
#
#
# ### NODE 生成的方法 
# - 第一階(Split query) ： query1, query2, ····
# - 第二階(Subquery) ： query1_main, query1_main_from1 ···· , query2_main, 
#   ····
# - 第三階(State) ：FROM、WHERE、GROUPBY 
#   ···· 
# - (Value, token) 與第三階一起 ：Value 記錄一個node 、 tokens各別記錄一個node
#   ···· 

# %%
    def __sassql_nodelist(self, sql_parsed_list):
        node_id = 0
        node_dict = {}
        node_property = [["node_id", # 0
                         "query_id", # 1
                         "subq_name", # 2
                         "state", # 3
                         "token_tag", # 4
                         "state_value", # 5
                         "state_alias", # 6
                         "state_parentname", # 7
                         "state_realname" # 8
                        ]]

        from_to_property = [["from", "to"]]

        for item in sql_parsed_list.items():
            ## initial var : Setting query_id, subquery_dict 
            ## 00_1 initial from_id and to_id To initial th relation by Different query
            query_id = item[0]
            subquery_dict = item[1].structured_dict
            from_id = ""
            to_id = ""

            # 01 Getting dict 
            ## 01_1 MAIN, MAIN_SELECT.....
            ## 01_2 "FROM", "WHERE", "GROUP BY", "HAVING", "SELECT", "ORDER BY", "LIMIT", "TABLE"
            ## 01_3 "token" get
            for item_subq in subquery_dict.items() : ## 01_1

                ## initial var : Get subquery name and Dict
                subq_name = item_subq[0]
                subq_value = item_subq[1]
                for state in Querystate.nodeorder_statelist : ## 01_2

                    ## initial var : one node property  
                    node_record = []

                    ## 01_2_0========================
                    ### SET Subquery (MAIN_...) TABLE to the subname
                    ### Check State In subquery_dict, if not continue
                    if re.search("MAIN_", subq_name) and state == "TABLE":
                        subq_value[ "TABLE" ] = {"value" : "SUBQ." + subq_name, 
                                                 "token" : [["", "SUBQ", subq_name]] }
                    if state not in subq_value.keys():
                        continue

                    ## 01_2_1=============================
                    ### Record Node property
                    ### 1. TABLE node has tokens, using only token node 
                    ### 2. From node has token and value need to be different NODE
                    node_id += 1
                    if state == "TABLE":
                        node_record = [ str(node_id), # 0
                                      query_id, # 1
                                      subq_name, # 2
                                      state, # 3
                                      "token", # 4
                                      subq_value[state]["value"], # 5
                                      self.__none_to_empty( subq_value[state]["token"][0][0] ), # 6
                                      self.__none_to_empty( subq_value[state]["token"][0][1] ), # 7
                                      self.__none_to_empty( subq_value[state]["token"][0][2] ) # 8
                                      ]
                    else:
                        node_record = [ str(node_id), # 0
                                      query_id, # 1
                                      subq_name, # 2
                                      state, # 3
                                      "value", # 4
                                      subquery_dict[subq_name][state]["value"], # 5
                                      "", # 6
                                      "", # 7
                                      "" # 8
                                      ]
                    node_property.append( node_record )
                    node_dict[ str(node_id) ] = node_record


                    ## 01_2_2=============================
                    ### Record relation (from to)
                    from_id, to_id = self.__from_to_check(from_id = from_id, to_id = to_id, now_id = node_id)

                    if from_id != "" and to_id != "" :
                        from_to_property.append([str(from_id), str(to_id)])


                    ## 01_3 =================================
                    ### Token gets 
                    ### Get value id be a to_id
                    if state == "FROM" :
                        to_id = from_id
                        for from_tb in subquery_dict[subq_name][state]["token"] :
                            node_id += 1
                            node_record = [ str(node_id), # 0
                                      query_id, # 1
                                      subq_name, # 2
                                      state, # 3
                                      "token", # 4
                                      "alias:" + self.__none_to_empty( from_tb[0] ) + ", " + "Database:" + self.__none_to_empty( from_tb[1] ) + ", " +  "Table:" + self.__none_to_empty( from_tb[2] ), # 5
                                      self.__none_to_empty( from_tb[0] ), # 6
                                      self.__none_to_empty( from_tb[1] ), # 7
                                      self.__none_to_empty( from_tb[2] )# 8
                                      ]
                            node_property.append( node_record )
                            node_dict[ str(node_id) ] = node_record

                            #### Record relation (from to)
                            from_id = node_id
                            if from_id != "" and to_id != "" :
                                from_to_property.append([str(from_id), str(to_id)])
        return(node_dict, node_property, from_to_property)

# %% [markdown]
# ## Query relation (NODE table相連 (state FROM and TABLE))

# %%
    def __query_relation(self, node_property) :
        node_df = pd.DataFrame( node_property[1:], columns = node_property[0] )
        fromtokens = node_df[ (node_df["state"] == "FROM") & (node_df["token_tag"] == "token") & (node_df["state_parentname"] != "SUBQ") ]
        tabletokens = node_df[ (node_df["state"] == "TABLE") & (node_df["token_tag"] == "token") & (node_df["state_parentname"] != "SUBQ")]
        sub_fromtokens = node_df[ (node_df["state"] == "FROM") & (node_df["token_tag"] == "token") & (node_df["state_parentname"] == "SUBQ") ]
        sub_tabletokens = node_df[ (node_df["state"] == "TABLE") & (node_df["token_tag"] == "token") & (node_df["state_parentname"] == "SUBQ") ]


        relation = pd.merge(tabletokens, fromtokens, on = ["state_parentname", "state_realname"], how = "inner")
        sub_relation = pd.merge(sub_tabletokens, sub_fromtokens, on = ["state_parentname", "state_realname"], how = "inner")

        from_to_query = [['from', 'to']]
        from_to_query = from_to_query + relation[["node_id_x", "node_id_y"]].values.tolist()
        from_to_query = from_to_query + sub_relation[["node_id_x", "node_id_y"]].values.tolist()

        return( from_to_query )
