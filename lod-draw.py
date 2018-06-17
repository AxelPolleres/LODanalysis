
# coding: utf-8


import json

data = []

#add your own json here, follow rthe pattern from 'test-lod-cloud.json'
fname = 'test-lod-cloud.json'
f = open(fname,'r')
data = json.loads(f.read())




import igraph as ig
import plotly.plotly as py
import plotly.tools as pyt
import plotly.graph_objs as go

L=len(data['links'])
N=len(data['nodes'])

Se= [ data['links'][k]['size'] for k in range(L) ] # size of edges
Ve= [ data['nodes'][k]['size'] for k in range(N) ] # size of vertices

#min and max pixel width for lines:
low_Se = 1
high_Se = 10

#min and max size for markers:
low_Ve = 20
high_Ve = 60

range_Se = max(Se)-min(Se)
range_Ve = max(Ve)-min(Ve)

#normalized sizes of edges
nSe = [ int(round(low_Se+(s-min(Se))/(range_Se)*(high_Se-low_Se))) for s in Se ]
nVe = [ int(round(low_Ve+(s-min(Ve))/(range_Ve)*(high_Ve-low_Ve))) for s in Ve ]

print("Normalized sizes of edges in pixel width:", nSe)
print("Normalized sizes of nodes in pixel width:",nVe)


G=ig.Graph()
for k in range(N):
    G.add_vertex(data['nodes'][k],size=nVe[k])
for k in range(L):
    G.add_edge(data['links'][k]['source'], data['links'][k]['target'],weight=nSe[k],directed=True)


labels=[]
group=[]
for node in data['nodes']:
    labels.append(node['name'])
    group.append(node['group'])
    


    

layt=G.layout('kk', dim=3)
Xn=[layt[k][0] for k in range(N)]# x-coordinates of nodes
Yn=[layt[k][1] for k in range(N)]# y-coordinates
Zn=[layt[k][2] for k in range(N)]# z-coordinates

traces =[]
for k in range(low_Se,high_Se+1):
    Xe=[]
    Ye=[]
    Ze=[]
    for e in G.es.select(weight_eq=k):
        print(e)
        Xe+=[layt[e.source][0],layt[e.target][0], None]# x-coordinates of edge ends
        Ye+=[layt[e.source][1],layt[e.target][1], None]
        Ze+=[layt[e.source][2],layt[e.target][2], None]


    trace = go.Scatter3d(x=Xe,
               y=Ye,
               z=Ze,
               mode='lines',
               line=dict(color='rgb(125,125,125)', width=k),
               hoverinfo='weight'
               )
    traces.append(trace)

#for k in range(low_Ve,high_Ve+1):
#    for v in G.vs.select(size_eq=k):
#        print(v)
trace = go.Scatter3d(x=Xn,
                     y=Yn,
                     z=Zn,
                     mode='markers+text',
                     name='name',
                     marker=dict(symbol='circle',
                     size=nVe,
                     color=group,
                     colorscale='Viridis',
                     line=dict(color='rgb(50,50,50)', width=0.5)),
                     text=labels,
                     textposition='top',
                     hoverinfo='text')
traces.append(trace)


axis=dict(showbackground=False,
          showline=False,
          zeroline=False,
          showgrid=False,
          showticklabels=False,
          title=''
          )

layout = go.Layout(
         title="3D visualization of '"+fname+"'",
         width=1000,
         height=1000,
         showlegend=False,
         scene=dict(
             xaxis=dict(axis),
             yaxis=dict(axis),
             zaxis=dict(axis),
        ),
     margin=dict(
        t=100
    ),
    hovermode='closest',
    annotations=[
           dict(
           showarrow=False,
            text="Data source: '"+fname+"'",
            xref='paper',
            yref='paper',
            x=0,
            y=0.1,
            xanchor='left',
            yanchor='bottom',
            font=dict(
            size=14
            )
            )
        ],    )


fig=go.Figure(data=traces, layout=layout)

pyt.set_credentials_file(username='droxel', api_key='C6l432lkagQHCwNqATKB')
py.iplot(fig, filename='test')


# In[189]:


import plotly
plotly.offline.plot(fig)

