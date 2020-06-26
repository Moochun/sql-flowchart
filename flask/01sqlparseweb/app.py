from flask import Flask, request, render_template
from sqlparsemodel.Sqlstructure import Sqlstructure
from sqlparsemodel.Sastructure import Sastructure
from sqlparsemodel.Mermaidplot import Sqltoflowchart
from sqlparsemodel.Querystate import Querystate
import re 
Querystate = Querystate()

app = Flask(__name__)



@app.route("/post_submit", methods=['GET', 'POST'])
def submit():
    if request.method == 'POST':
        query_o = request.form.get('query')
        query = re.sub("\s+?;",";",query_o)
        query = re.sub("(\n)|(\r)|(\t)"," ",query)
        query = re.sub("\\/\\*.*?\\*\\/", "",query)

        # query to Structure List to SAS node relation 
        sas_query = Sastructure(query)

        # Node relation Setting and Plot the mermaid structure
        sfc = Sqltoflowchart(sas_query.from_to_property,
         sas_query.from_to_query,
         sas_query.node_property )

        # 1. Ignore Some Relation words 
        # 2. Draw some node to colors 
        # 3. Draw the query to colors 
        sfc.mermaid_plot(relation_ignore = [])
        ## FOR SAS CODE 
        sfc.mermaid_drawnode_containtxt(keytxt = "DATA_STEP_FLOWCHART", colname = "state_value", color = "#00FFFF", allquery_bool = True)
        sfc.mermaid_drawnode_containtxt(keytxt = "PROC_TRANSPOSE_FLOWCHART", colname = "state_value", color = "#00FFFF", allquery_bool = True)
        sfc.mermaid_drawnode_containtxt(keytxt = "PROC_SORT_FLOWCHART", colname = "state_value", color = "#00FFFF", allquery_bool = True)
        ## FOR ALL CODE
        sfc.mermaid_drawnode(keyword = ["WORK","","SUBQ"], token_tag = "token", color = "#F0BBFF", equalkeyword = "notsame")
        sfc.mermaid_drawnode(keyword = "DROP", token_tag = "token", color = "#AAAAAA", equalkeyword = "contain")
        sfc.mermaid_drawnode(keyword = "QUERY", token_tag = "token", color = "#28FF28")
        sfc.mermaid_drawnode(keyword = "CR20", token_tag = "token", color = "#FFAA33")

        sfc.mc.mermaid_txt = re.sub("\n|\r", "",sfc.mc.mermaid_txt)        
        sfc.mc.mermaid_txt = re.sub(";", ";\n",sfc.mc.mermaid_txt)
        return(render_template('post_submit.html', mermaid = sfc.mc.mermaid_txt, remindtext = query_o))
    return render_template('post_submit.html', remindtext = "SAS Query...")


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
