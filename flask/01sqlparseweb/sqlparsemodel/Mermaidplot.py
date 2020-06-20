#!/usr/bin/env python
# coding: utf-8
# %% [markdown]
# # Mermaid_Plots
# ## Mermaid_creator
# ### Close Sentence
# ### Add Node
# ### Add Link
# ### Subgraphs show
# ### Setting Node Color 

# %%
import re

# %%


class Mermaid_creator():
    
    def __init__(self):
        self.mermaid_txt = "graph TD;"
    
    # 01 Close Sentence
    def close_sentence(self):
        self.mermaid_txt += ";"
    
    # 02 Add Node    
    def add_node(self, shape = "", txt = "", node_id = ""):
        # Setting node id 
        self.mermaid_txt += str(node_id)
        
        # Setting texts
        if shape == "rectangular":
            self.mermaid_txt += "[" + txt +"]"
            
        elif shape == "roundedges":
            self.mermaid_txt += "(" + txt +")"
            
        elif shape == "circle":
            self.mermaid_txt += "((" + txt +"))"
            
        elif shape == "tag":
            self.mermaid_txt += ">" + txt +"]"
            
        elif shape == "diamond":
            self.mermaid_txt += "{" + txt +"}" 
            
            
    # 03 Add Link        
    def add_link(self, shape = "", txt = ""):
        
        # Setting relation texts
        if shape == "":
            print("please set a shape for line")
        
        # 01 Arrowhead
        elif shape == "arrowhead" :
            if txt == "" : 
                self.mermaid_txt += "-->"
            else:
                self.mermaid_txt += "-->" + "|" + txt + "|"
        
        # 02 Openlink
        elif shape == "openlink" :
            if txt == "" : 
                self.mermaid_txt += "---"
            else:
                self.mermaid_txt += "---" + "|" + txt + "|"
                
        # 03 Dotted link
        elif shape == "dottedlink" :
            if txt == "" : 
                self.mermaid_txt += "-.->"
            else:
                self.mermaid_txt += "-." + txt + ".->"
        
        # 03 Thick link
        elif shape == "thicklink" :
            if txt == "" : 
                self.mermaid_txt += "==>"
            else:
                self.mermaid_txt += "==" + txt + "==>"
                
    # 04 Subgraphs        
    def subgraph(self, state = "", txt = ""):
        # Setting relation texts
        if state == "" :
            print("please set a shape and text for subgraph")
            
        if state == "start":
            self.mermaid_txt += "subgraph " + txt + ";"
            
        if state == "end" and txt == "":
            self.mermaid_txt += "end; "
            
    # 05 Setting Node Color
    def add_node_color(self, node_id, color) :
        # Setting node Color
        self.mermaid_txt += " style " + node_id + " fill:" + color + ";" # live editor no need setting \n


# %%

# mc = Mermaid_creator()

# mc.subgraph(state = "start", txt = "123456789")
# mc.add_node(shape = "rectangular", txt = "First Node", node_id = "1")
# mc.add_link(shape = "arrowhead", txt = "")
# mc.add_node(shape = "rectangular", txt = "Second Node", node_id = "2")
# mc.close_sentence()

# mc.add_node(shape = "roundedges", txt = "Third Node", node_id = "3")
# mc.add_link(shape = "arrowhead", txt = "Some relations")
# mc.add_node(shape = "rectangular", txt = "Second Node", node_id = "2")
# mc.close_sentence()
# mc.subgraph(state = "end")

# mc.add_node(shape = "rectangular", txt = "Second Node", node_id = "2")
# mc.add_link(shape = "dottedlink", txt = "table connect")
# mc.add_node(shape = "diamond", txt = "Forth Node", node_id = "4")
# mc.close_sentence()

# mc.add_node(node_id = "4")
# mc.add_link(shape = "dottedlink", txt = "table connect")
# mc.add_node(node_id = "5")
# mc.close_sentence()
# mc.mermaid_txt


# %% [markdown]
# ## Sqltoflowchart (Node to Mermaid)
# ### Create node txt
# ### Create relation txt
# ### Create mermaid plot
# #### Initial setting from and to node setting 
# #### Subgraph start drawing(from_node)
# #### From node create (from_node)
# #### From relation create (from_node)
# #### To node create (to_node)
# #### End Line
# #### Subgraph end drawing (to_node)
#
# ### Draw mermaid plot

# %%


import pandas as pd
class Sqltoflowchart():
    
    def __init__(self, from_to_node, from_to_query, node_property):
        self.from_to_node = from_to_node
        self.from_to_query = from_to_query
        self.node_property = pd.DataFrame(node_property[1:], columns= node_property[0])
        self.mc = Mermaid_creator()
    
    # 00_1 regrex Setting (upper and lower no different)
    def upper_lower_regrex(self, string):
        regex_txt = ""
        for c in string:
            if c == " " : 
                regex_txt += " "
            else:
                regex_txt += "[" + c.lower() + c.upper() + "]"
        return(regex_txt)
    
    # 01 Create node txt
    def Create_nodetxt(self, node_id, node):
        token_tag = node["token_tag"].tolist()[0]
        node_value = ""
        node_value += "<b>" + node["state"].tolist()[0] + "</b>"
        
        if token_tag == "token" :
            node_value += "<br>" + node["state_parentname"].tolist()[0] + "." + node["state_realname"].tolist()[0] + ", " + node["state_alias"].tolist()[0]
        
        ## Add <center> </center>
        node_value = "<center>" + node_value + "</center>"
        
        return node_value
    
    # 02 Create relation txt
    def Create_relationtxt(self, node_id, node):
        token_tag = node["token_tag"].tolist()[0]
        relation_value = ""
        
        if token_tag == "token" : 
            print("Empty relation (Class Sqltoflowchart - def Create_relationtxt)")
        else :
            relation_value = node[ "state_value" ].tolist()[0]
            relation_value = re.sub("\\(|\\)", " ", relation_value) # Live Editor can get bracket "(" ")"
            relation_value = re.sub(";", "", relation_value)
            relation_value = re.sub(self.upper_lower_regrex("INNER JOIN "), "<b>INNER JOIN </b>", relation_value)
            relation_value = re.sub(self.upper_lower_regrex("LEFT JOIN "), "<b>LEFT JOIN </b>", relation_value)
            relation_value = re.sub(self.upper_lower_regrex("RIGHT JOIN "), "<b>RIGHT JOIN </b>", relation_value)
            relation_value = re.sub(self.upper_lower_regrex("OUTER JOIN "), "<b>OUTER JOIN </b>", relation_value)
            relation_value = re.sub(self.upper_lower_regrex("CROSS JOIN "), "<b>CROSS JOIN </b>", relation_value)
            relation_value = re.sub(self.upper_lower_regrex("DISTINCT "), "<b>DISTINCT </b>", relation_value)
            relation_value = re.sub(self.upper_lower_regrex("ON "), "ON <br>", relation_value)
            relation_value = re.sub(self.upper_lower_regrex("AND "), "AND <br>", relation_value)
            if node[ "state" ].tolist()[0] in ["SELECT"] :
                relation_value = re.sub(self.upper_lower_regrex(","), ", <br>", relation_value)
#             print("Need txt relation (Class Sqltoflowchart - def Create_relationtxt)")
        ## Add <center> </center>
        relation_value = "<center>" + relation_value + "</center>" # Live Editor can get bracket "(" ")"
        
        return(relation_value)    
    
    #03 Create mermaid plot
    def mermaid_plot(self, relation_ignore = []):
        #### 03_2 Initial Subgraph start drawing
        subgraph_sign = 0 # Record the subgraph , start->set 1, end->set 0
        
        
        #01 Node relation create
        for from_to in self.from_to_node :
            from_id = ""
            to_id = ""
            from_node = ""
            to_node = ""
            node_txt = ""
            relation = ""
            
            if from_to[0] == "from" :
                print("columnnames" + str(from_to))
                continue
            
            
            # 03_1 Initial setting from and to node setting 
            from_id = from_to[0]
            to_id = from_to[1]
            from_node = self.node_property[self.node_property["node_id"] == from_id]
            to_node = self.node_property[self.node_property["node_id"] == to_id]
            
            #03_2 Subgraph start drawing(from_node) ===============================
            
            if from_node["state"].tolist()[0] == "FROM" and from_node["token_tag"].tolist()[0] == "token" and subgraph_sign == 0 :
                subgraph_sign = 1
                self.mc.subgraph(state = "start", txt = "<needtobereplacetable>")
                
                
            #03_3 From node create (from_node)===============================
            node_txt = self.Create_nodetxt(node_id = from_id, node = from_node)
            if from_node["token_tag"].tolist()[0] == "token" :
                self.mc.add_node(shape = "roundedges", txt = node_txt, node_id = from_id)
            else : 
                self.mc.add_node(shape = "diamond", txt = node_txt, node_id = from_id)
            
            
            #03_4 From relation create (from_node) (Ignore Show by param : relation_ignore)===============================
            relation_txt = self.Create_relationtxt(node_id = from_id, node = from_node)
            if from_node["token_tag"].tolist()[0] == "token" or from_node["state"].tolist()[0] in relation_ignore: # Setting relation Ignore show
                self.mc.add_link(shape = "arrowhead", txt = "")
            else : 
                self.mc.add_link(shape = "arrowhead", txt = relation_txt)
            
            
            
            #03_5 To node create (to_node)===============================
            node_txt = self.Create_nodetxt(node_id = to_id, node = to_node)
            if to_node["token_tag"].tolist()[0] == "token" :
                self.mc.add_node(shape = "roundedges", txt = node_txt, node_id = to_id)
            else : 
                self.mc.add_node(shape = "diamond", txt = node_txt, node_id = to_id)
            
            #03_6 End Line ===============================
            self.mc.close_sentence()
            
            
            #03_7 Subgraph end drawing (to_node) ===============================
            ## 01 "TABLE" state, "token" token_tag setting end line
            ## 02 Replace the  mermaid_txt <needtobereplacetable> replace to TABLE name 
            if to_node["state"].tolist()[0] == "TABLE" and to_node["token_tag"].tolist()[0] == "token" and subgraph_sign == 1 :
                subgraph_sign = 0
                self.mc.subgraph(state = "end")
                self.mc.mermaid_txt = re.sub("\\<needtobereplacetable\\>", to_node["state_value"].tolist()[0], self.mc.mermaid_txt)
                print("replace relation (Class Sqltoflowchart - def Create_relationtxt)")
            
        #02 query relation create
        for from_to_query in self.from_to_query :
            from_id = ""
            to_id = ""
            if from_to_query[0] == "from" :
                print("columnnames" + str(from_to_query))
                continue
            print("columnnames" + str(from_to_query))
            # 04_1 Initial setting from and to node setting 
            from_id = from_to_query[0]
            to_id = from_to_query[1]
            
            self.mc.add_node(node_id = from_id)
            self.mc.add_link(shape = "dottedlink", txt = "")
            self.mc.add_node(node_id = to_id)
            self.mc.close_sentence()
            
    #04 Draw mermaid plot
    def mermaid_drawnode(self, keyword, token_tag, color):
        draw_nodes = self.node_property[(self.node_property["state_parentname"] == keyword) & (self.node_property["token_tag"] == token_tag)]
        for node_id in draw_nodes["node_id"]:
            self.mc.add_node_color(node_id, color)


# %%


# sfc = Sqltoflowchart(sas_query.from_to_property,
# sas_query.from_to_query,
# sas_query.node_property)
# sas_query.from_to_query
# sfc.mermaid_plot()
# sfc.mermaid_drawnode(keyword = "QUERY", token_tag = "token", color = "#28FF28")
# sfc.mc.mermaid_txt


# %%




