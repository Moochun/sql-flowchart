from flask import Flask, request, render_template
from sqlparsemodel.Sqlstructure import Sqlstructure
from sqlparsemodel.Sastructure import Sastructure
from sqlparsemodel.Mermaidplot import Sqltoflowchart
from sqlparsemodel.Querystate import Querystate
import re 
Querystate = Querystate()

app = Flask(__name__)

@app.route('/')
def index():
    return 'hello man'

@app.route("/post_submit", methods=['GET', 'POST'])
def submit():
    if request.method == 'POST':
        query_o = request.form.get('query')
        query = re.sub("\\/.*?\\/", "",query_o)

        # query to Structure List to SAS node relation 
        sas_query = Sastructure(query)

        # Node relation Setting and Plot the mermaid structure
        sfc = Sqltoflowchart(sas_query.from_to_property,
         sas_query.from_to_query,
         sas_query.node_property )

        # 1. Ignore Some Relation words 
        # 2. Draw some node to colors 
        sfc.mermaid_plot(relation_ignore = [])
        sfc.mermaid_drawnode(keyword = "QUERY", token_tag = "token", color = "#28FF28")
        sfc.mermaid_drawnode(keyword = "CR20", token_tag = "token", color = "#FFAA33")
        return(render_template('post_submit.html', mermaid = sfc.mc.mermaid_txt, remindtext = query_o))
    return render_template('post_submit.html', remindtext = "SAS Query...")


if __name__ == '__main__':
    app.debug = True
    app.run()