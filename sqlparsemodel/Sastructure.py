#!/usr/bin/env python
# coding: utf-8
# %% [markdown]
# # CLASS SAS Structure

# %%
from sqlparsemodel.Sqlstructure import Sqlstructure
from sqlparsemodel.Querystate import Querystate
from sqlparsemodel.Mermaidplot import Sqltoflowchart
import re 
import pandas as pd 
import copy
Querystate = Querystate()
sfc = Sqltoflowchart()

# %% [markdown]
# ## init function 
#
#


# %%
class Sastructure():
    
    def __init__(self, query):
        self.sql_parsed_list, self.drop_dict = self.__sassql_parsed( query )
        self.node_dict, self.node_property, self.from_to_property = self.__sassql_nodelist( self.sql_parsed_list )
        self.node_property = pd.DataFrame( self.node_property[1:], columns = self.node_property[0] )
        self.__node_property_change()
        self.from_to_query = self.__query_relation( self.node_property )
        
        

# %% [markdown]
# ## From to ID check and replace 
# - 先放from_id : from_id == "" and to_id == "" 把now_id放到from_id
# - 再放to_id : from_id != "" and to_id == "" 把now_id放到to_id
# - id置換 : from_id != "" and to_id != "" to_id 取代 from_id，並把now_id放到_to_id
#
# ## DATASTEP PROCESS
# ### PROC SQL
# ### DATA STEP
# ### PROC TRANSPOSE
# ### PROC SORT

# %%
    def __node_property_change(self):
        
        # 01Upper db name and table name to connect the query relation 
        self.node_property["state_parentname"] = self.node_property["state_parentname"].str.upper() # upper db name 
        self.node_property["state_realname"] = self.node_property["state_realname"].str.upper()# upper table name
        
        # 02 Subquery table value need to Add Main query table name before
        ## 02-1 get query id -> table name 
        ## 02-2 subq name -> bind the table name and subq name
        ## 02-3 find node id and Replace the value in Pandas
        subq_nodes = self.node_property[(self.node_property["subq_name"].str.contains("MAIN_")) & (self.node_property["state"] == "TABLE")]
        for idx in range(len(subq_nodes)):
            node_id = subq_nodes.iloc[idx]["node_id"]
            query_id = subq_nodes.iloc[idx]["query_id"]
            subq_name = subq_nodes.iloc[idx]["state_value"]
            table_name = self.node_property[(self.node_property["query_id"] == query_id) & (self.node_property["subq_name"] == "MAIN") & (self.node_property["state"] == "TABLE")]["state_value"].values[0] # pandas get values to be string
            final_name = table_name + "--" + subq_name
            self.node_property.at[self.node_property["node_id"] == node_id, "state_value"] = final_name
            
        # 03 DROP tables renames token and value (self.drop_dict)
        ## 03_1 Revised the dbname from origin to DROP....
        ## 03_2 2 kind of TABLE NAME. (First find the drop table name)
            ### 03_2_1 WORK or No DB Nname -> search "" and "WORK" replace to DROP...
            ### 03_2_2 Other -> search "<DBNAME>" to DROP.... 
        for item in self.drop_dict.items():
            key_dbname = item[0]
            value_dropnames = item[1]
            dropname_list = re.split("\\.", re.sub("WORK\\.", "",value_dropnames["drop_name"]))
            
            print("SASstructure:__node_property_change:DROP tables renames============================")
            print(key_dbname)
            print(value_dropnames)
            print(dropname_list)
            kind1_bool = ""
            kind2_bool = ""
            kind3_bool = ""
            
            if len(dropname_list) == 1 :
                kind1_bool = self.node_property["query_id"].isin(value_dropnames["query_ids"]) & (self.node_property["state_parentname"] == "") & (self.node_property["state_realname"] == dropname_list[0].upper())
                kind2_bool = self.node_property["query_id"].isin(value_dropnames["query_ids"]) & (self.node_property["state_parentname"] == "WORK") & (self.node_property["state_realname"] == dropname_list[0].upper())
                
                self.node_property.at[kind1_bool , "state_parentname"] = key_dbname
                self.node_property.at[kind2_bool , "state_parentname"] = key_dbname
                self.node_property.at[kind1_bool, "state_value"] = self.node_property[kind1_bool]["state_value"].str.cat(["," + key_dbname]*kind1_bool.sum())
                self.node_property.at[kind2_bool, "state_value"] = self.node_property[kind2_bool]["state_value"].str.cat(["," + key_dbname]*kind2_bool.sum())
                
                
            elif len(dropname_list) == 2 :
                kind3_bool = self.node_property["query_id"].isin(value_dropnames["query_ids"]) & (self.node_property["state_parentname"] == dropname_list[0].upper()) & (self.node_property["state_realname"] == dropname_list[1].upper())
                self.node_property.at[kind3_bool , "state_parentname"] = key_dbname
                self.node_property.at[kind3_bool, "state_value"] = self.node_property[kind3_bool]["state_value"].str.cat(["," + key_dbname]*kind3_bool.sum())
        
        

        

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
    def __dbfromname_tosql(self, dbname, from_name):
        tosql = "CREATE TABLE "+ dbname + " FROM "
        from_list = re.split("\s+", from_name)
        tosql += from_list[0]
        if len(from_list) > 1 :
            for idx in range(1,len(from_list)) : 
                tosql += " merge " + from_list[idx]
            
        return( tosql )

# %%
    def __sassql_parsed(self, query):
        sql_parsed_list = {}
        query_id = 0
        
        ## Get SAS Different Statetments
        ### Let ";" no space infront of it 
        ### Let the "\n, \r" to Space to (regex)
        query = re.sub("\s+?;",";",query)
        query = re.sub("(\n)|(\r)"," ",query)
        ## 1. PROC SQL
        ## 2. DATA STEP
        ## 3. PROC TRANSPOSE
        ## 4. PROC SORT
        proc_sql = re.findall(sfc.upper_lower_regrex("PROC") + "\s+" + sfc.upper_lower_regrex("SQL") + ".*?" + sfc.upper_lower_regrex("QUIT;"), query)# Get PROC SQL be the SQL string
        data_step = re.findall(sfc.upper_lower_regrex("DATA") + "\s+(?!=).*?" + sfc.upper_lower_regrex("RUN;"), query) # If the query with "=" after "data" not the Data Step target
        proc_transpose = re.findall(sfc.upper_lower_regrex("PROC") + "\s+" + sfc.upper_lower_regrex("TRANSPOSE") + ".*?" + sfc.upper_lower_regrex("RUN;"), query)
        proc_sort = re.findall(sfc.upper_lower_regrex("PROC") + "\s+" + sfc.upper_lower_regrex("SORT") + ".*?" + sfc.upper_lower_regrex("RUN;"), query)
        
        ### If Just Sql not the SAS SQL set to all string
        if len(proc_sql) == 0 and len(data_step) == 0 and len(proc_transpose) == 0 and len(proc_sort) == 0 : 
            proc_sql = [query]
        
        
        ### 1. PROC SQL : Catch detail sql to flowchart 
        #### 1. CREATE .... ; 2. SELECT ..... ; 3. DROP .....; 
        proc_sql_detail = []
        for sql in proc_sql :
            proc_sql_detail = proc_sql_detail + re.findall("\s+" + sfc.upper_lower_regrex("CREATE") + ".*?;" + "|" + "\s+" + sfc.upper_lower_regrex("SELECT") + ".*?;" + "|" + "\s+" + sfc.upper_lower_regrex("DROP") + ".*?;", sql)
            
        
            
        ###  1. PROC SQL catch flow ==========================================
        drop_dict = {}
        drop_queryids = []
        drop_count = 0
        for string in proc_sql_detail :
           
            
            string = re.sub(sfc.upper_lower_regrex("PROC") + "\s+" + sfc.upper_lower_regrex("SQL;"), "", string)
            
            ### (Record Drop) to the dictionary for node property revised
            if re.findall("\s+" + sfc.upper_lower_regrex("DROP") + ".*?;", string) and len(drop_queryids) != 0 :
                drop_count += 1
                drop_tb_name = re.findall(sfc.upper_lower_regrex("DROP") + "\s+" + sfc.upper_lower_regrex("TABLE") + "(.*?);", string)
                drop_tb_name = drop_tb_name[0].strip()
                drop_dict["DROP" + str(drop_count)] = {"drop_name" : drop_tb_name, 
                                                       "query_ids" : copy.deepcopy(drop_queryids) }
                
            ## String is empty or is DROP QUERY ->  ignore
            if string.strip() == "" or len(re.findall("\s+" + sfc.upper_lower_regrex("DROP") + ".*?;", string)) > 0:
                continue
            
            ## Sql structure show
            query_id += 1
            sql_parsed_list[ str(query_id) ] = Sqlstructure(string)
            
            ### (Record Drop) query_id for node property revised
            drop_queryids.append(str(query_id))
            
            
            # Only Select -> get MAIN query and Set QUERYID.query <query_id> be the TABLE name
            if "TABLE" not in list( sql_parsed_list[ str(query_id) ].structured_dict['MAIN'].keys() ):
                sql_parsed_list[ str(query_id) ].structured_dict['MAIN']['TABLE'] = {'token' : [['', 'NOCREATETB', 'query' + str(query_id)]], 'value': 'NOCREATETB.query' + str(query_id)}
        
        
        ###  2. DATA STEP catch flow ==========================================
        for string in data_step :
            query_id += 1
            table_name = ""
            from_name = ""
            print("2. data_step================= query_id" + str(query_id))
            ## Get Table name and Out name
            ### if out name is empy -> same as table name
            table_name = re.findall(sfc.upper_lower_regrex("DATA") + "(.*?);", string)
            table_name = table_name[0].strip()
            from_name = re.findall(sfc.upper_lower_regrex("SET") + "(.*?);", string)
            if len(from_name) == 0 : # SET no using MERGE
                from_name = re.findall(sfc.upper_lower_regrex("MERGE") + "(.*?);", string)  
                
            if len(from_name) == 0 :
                from_name = table_name
            else :
                from_name = from_name[0].strip()   
                
            ### if no dbname set -> WORK
            if len(re.findall("\\.", table_name)) == 0 :
                table_name = "WORK." + table_name
            from_list = re.split("\s+", from_name)
            from_name = ""
            for from_elem in from_list:
                if len(re.findall("\\.", from_elem)) == 0 : # Add WORK 
                    from_name += "WORK." + from_elem + " "
                else : # No ADD WORK
                    from_name += from_elem + " "
            from_name = from_name.strip()
            
            

            string = self.__dbfromname_tosql(re.sub(";", "",table_name), re.sub(";", "",from_name))
            string += " AND(DATA_STEP_FLOWCHART)"
            sql_parsed_list[ str(query_id) ] = Sqlstructure(string)
            
            

        ###  3. PROC TRANSPOSE catch flow ==========================================
        for string in proc_transpose :
            query_id += 1
            table_name = ""
            from_name = ""
            print("3. proc_transpose=================")
            # data 
            ## Replace some special sign, ex: "(", ")" -> space
            ### TABLE NAME GET
            ### FROM NAME GET
            rtmp = re.sub("\\(|\\)", " ", string)
            # trans_sign = PROC\s+TRANSPOSE\s+DATA
            trans_sign = sfc.upper_lower_regrex("PROC") + "\s+" + sfc.upper_lower_regrex("TRANSPOSE")+ "\s+" + sfc.upper_lower_regrex("DATA")
            from_name = re.findall(trans_sign + "\s*?=(.*?)\s+", rtmp)
            table_name = re.findall(trans_sign + ".*?" + sfc.upper_lower_regrex("out") + "\s*?=(.*?);", rtmp)
            table_name = table_name[0].strip()
            if len(from_name) == 0 :
                from_name = table_name
            else :
                from_name = re.split(" ",re.sub(";", " ",from_name[0]).strip())[0]
                
                
             ### if no dbname set -> WORK    
            if len(re.findall("\\.", table_name)) == 0 :
                table_name = "WORK." + table_name
            if len(re.findall("\\.", from_name)) == 0 :
                from_name = "WORK." + from_name
                
            
            string = self.__dbfromname_tosql(re.sub(";", "",table_name), re.sub(";", "",from_name))
            string += " AND(PROC_TRANSPOSE_FLOWCHART)"
            sql_parsed_list[ str(query_id) ] = Sqlstructure(string)
        
        ###  4. PROC SORT catch flow
        for string in proc_sort :
            query_id += 1
            table_name = ""
            from_name = ""
            print("4. proc_sort=================")
            # data 
            ## Replace some special sign, ex: "(", ")" -> space
            ### TABLE NAME GET
            ### FROM NAME GET
            rtmp = re.sub("\\(|\\)", " ", string)
            sort_sign = sfc.upper_lower_regrex("PROC") + "\s+" + sfc.upper_lower_regrex("SORT")+ "\s+" + sfc.upper_lower_regrex("DATA")
            from_name = re.findall(sort_sign + "\s*?=(.*?)\s+", rtmp)
            table_name = re.findall(sort_sign + ".*?" + sfc.upper_lower_regrex("out") + "\s*?=(.*?);", rtmp)
            table_name = table_name[0].strip()
            if len(from_name) == 0 :
                from_name = table_name
            else :
                from_name = re.split(" ",re.sub(";", " ",from_name[0]).strip())[0]
            
            
            ### if no dbname set -> WORK
            if len(re.findall("\\.", table_name)) == 0 :
                table_name = "WORK." + table_name
            if len(re.findall("\\.", from_name)) == 0 :
                from_name = "WORK." + from_name
            
            
            
            string = self.__dbfromname_tosql(re.sub(";", "",table_name), re.sub(";", "",from_name))
            string += " AND(PROC_SORT_FLOWCHART)"
            sql_parsed_list[ str(query_id) ] = Sqlstructure(string)
        
        return( sql_parsed_list , drop_dict)

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
            query_id = item[0]
            subquery_dict = item[1].structured_dict

            # 01 Getting dict 
            ## 01_1 MAIN, MAIN_SELECT.....
            ## 01_2 "FROM", "WHERE", "GROUP BY", "HAVING", "SELECT", "ORDER BY", "LIMIT", "TABLE"
            ## 01_3 "token" get
            for item_subq in subquery_dict.items() : ## 01_1
                
                ## 01_1 initial from_id and to_id To initial th relation by Different query
                from_id = ""
                to_id = ""
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
        node_df = node_property
        fromtokens = node_df[ (node_df["state"] == "FROM") & (node_df["token_tag"] == "token") & (node_df["state_parentname"] != "SUBQ") ]
        tabletokens = node_df[ (node_df["state"] == "TABLE") & (node_df["token_tag"] == "token") & (node_df["state_parentname"] != "SUBQ")]
        sub_fromtokens = node_df[ (node_df["state"] == "FROM") & (node_df["token_tag"] == "token") & (node_df["state_parentname"] == "SUBQ") ]
        sub_tabletokens = node_df[ (node_df["state"] == "TABLE") & (node_df["token_tag"] == "token") & (node_df["state_parentname"] == "SUBQ") ]
        
        # Setting TABLE name to uppper value for Relation
        fromtokens["state_realname"] = fromtokens["state_realname"].str.upper()
        fromtokens["state_parentname"] = fromtokens["state_parentname"].str.upper()
        tabletokens["state_realname"] = tabletokens["state_realname"].str.upper()
        tabletokens["state_parentname"] = tabletokens["state_parentname"].str.upper()

        relation = pd.merge(tabletokens, fromtokens, on = ["state_parentname", "state_realname"], how = "inner")
        relation = relation[relation["query_id_x"] != relation["query_id_y"]] # No loop relation in one query 
        sub_relation = pd.merge(sub_tabletokens, sub_fromtokens, on = ["query_id", "state_parentname", "state_realname"], how = "inner")

        from_to_query = [['from', 'to']]
        from_to_query = from_to_query + relation[["node_id_x", "node_id_y"]].values.tolist()
        from_to_query = from_to_query + sub_relation[["node_id_x", "node_id_y"]].values.tolist()

        return( from_to_query )
